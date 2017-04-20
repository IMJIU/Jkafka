package kafka.network;


import com.yammer.metrics.core.Meter;
import kafka.utils.SystemTime;
import kafka.utils.Time;

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Thread that processes all requests from a single connection. There are N of these running in parallel
 * each of which has its own selectors
 */
public class Processor extends AbstractServerThread {
    public Integer id;
    public Time time;
    public Integer maxRequestSize;
    public Meter aggregateIdleMeter;
    public Meter idleMeter;
    public Integer totalProcessorThreads;
    public RequestChannel requestChannel;
    public ConnectionQuotas connectionQuotas;
    public Long connectionsMaxIdleMs;

    private ConcurrentLinkedQueue<SocketChannel> newConnections = new ConcurrentLinkedQueue<SocketChannel>();
    private Long connectionsMaxIdleNanos = connectionsMaxIdleMs * 1000 * 1000;
    private Long currentTimeNanos = System.nanoTime();
    private LinkedHashMap<SelectionKey, Long> lruConnections = new LinkedHashMap<>();
    private Long nextIdleCloseCheckTime = currentTimeNanos + connectionsMaxIdleNanos;

    public Processor(Integer id, Time time, Integer maxRequestSize, Meter aggregateIdleMeter, Meter idleMeter, Integer totalProcessorThreads, RequestChannel requestChannel, ConnectionQuotas connectionQuotas, long connectionsMaxIdleMs) {
        this.id = id;
        this.time = time;
        this.maxRequestSize = maxRequestSize;
        this.aggregateIdleMeter = aggregateIdleMeter;
        this.idleMeter = idleMeter;
        this.totalProcessorThreads = totalProcessorThreads;
        this.requestChannel = requestChannel;
        this.connectionQuotas = connectionQuotas;
        this.connectionsMaxIdleMs = connectionsMaxIdleMs;
    }


    public void run() {
        startupComplete();
        while (isRunning()) {
            // setup any new connections that have been queued up;
            configureNewConnections();
            // register any new responses for writing;
            processNewResponses();
            Long startSelectTime = time.nanoseconds();
            int ready = selector.select(300);
            currentTimeNanos = time.nanoseconds();
            Long idleTime = currentTimeNanos - startSelectTime;
            idleMeter.mark(idleTime);
            // We use a single meter for aggregate idle percentage for the thread pool.;
            // Since meter is calculated as total_recorded_value / time_window and;
            // time_window is independent of the number of threads, each recorded idle;
            // time should be discounted by # threads.;
            aggregateIdleMeter.mark(idleTime / totalProcessorThreads);

            trace("Processor id " + id + " selection time = " + idleTime + " ns");
            if (ready > 0) {
                Set<SelectionKey> keys = selector.selectedKeys();
                Iterator<SelectionKey> iter = keys.iterator();
                while (iter.hasNext() && isRunning()) {
                    SelectionKey key = null;
                    try {
                        key = iter.next();
                        iter.remove();
                        if (key.isReadable())
                            read(key);
                        else if (key.isWritable())
                            write(key);
                        else if (!key.isValid())
                            close(key);
                        else ;
                        throw new IllegalStateException("Unrecognized key state for processor thread.");
                    } catch (EOFException e) {
                        info(String.format("Closing socket connection to %s.", channelFor(key).socket().getInetAddress()));
                        close(key);
                    } catch (InvalidRequestException e) {
                        info(String.format("Closing socket connection to %s due to invalid request: %s", channelFor(key).socket.getInetAddress, e.getMessage))
                        close(key);
                    } catch (Exception e) {
                        error("Closing socket for " + channelFor(key).socket().getInetAddress + " because of error", e)
                        close(key);
                    }
                }
            }
            maybeCloseOldestConnection();
        }

        debug("Closing selector.");

        closeAll();

        swallowError(() -> selector.close());

        shutdownComplete();
    }

    /**
     * Close the given key and associated socket
     */
    @Override
    public void close(SelectionKey key) {
        lruConnections.remove(key);
        super.close(key);
    }

    private void processNewResponses() {
        var curr = requestChannel.receiveResponse(id);
        while (curr != null) {
            SelectionKey key = (SelectionKey)curr.request.requestKey;
            try {
                curr.responseAction match {
                    case RequestChannel.NoOpAction =>{
                        // There is no response to send to the client, we need to read more pipelined requests;
                        // that are sitting in the server's socket buffer;
                        curr.request.updateRequestMetrics;
                        trace("Socket server received empty response to send, registering for read: " + curr)
                        key.interestOps(SelectionKey.OP_READ);
                        key.attach(null);
                    }
                    case RequestChannel.SendAction =>{
                        trace("Socket server received response to send, registering for write: " + curr)
                        key.interestOps(SelectionKey.OP_WRITE);
                        key.attach(curr);
                    }
                    case RequestChannel.CloseConnectionAction =>{
                        curr.request.updateRequestMetrics;
                        trace("Closing socket connection actively according to the response code.");
                        close(key);
                    }
                    case responseCode =>throw new KafkaException("No mapping found for response code " + responseCode)
                }
            } catch {
                case CancelledKeyException e =>{
                    debug("Ignoring response for closed socket.")
                    close(key);
                }
            }finally{
                curr = requestChannel.receiveResponse(id);
            }
        }
    }

    /**
     * Queue up a new connection for reading
     */
    public void accept(SocketChannel socketChannel) {
        newConnections.add(socketChannel);
        wakeup();
    }

    /**
     * Register any new connections that have been queued up
     */
    privatepublic

    void configureNewConnections() {
        while (newConnections.size() > 0) {
            val channel = newConnections.poll();
            debug("Processor " + id + " listening to new connection from " + channel.socket.getRemoteSocketAddress);
            channel.register(selector, SelectionKey.OP_READ);
        }
    }

    /*
     * Process reads from ready sockets
     */
    public void read(SelectionKey key) {
        lruConnections.put(key, currentTimeNanos);
        val socketChannel = channelFor(key);
        var receive = key.attachment.asInstanceOf < Receive >
        if (key.attachment == null) {
            receive = new BoundedByteBufferReceive(maxRequestSize);
            key.attach(receive);
        }
        val read = receive.readFrom(socketChannel);
        val address = socketChannel.socket.getRemoteSocketAddress();
        trace(read + " bytes read from " + address);
        if (read < 0) {
            close(key);
        } else if (receive.complete) {
            val req = RequestChannel.Request(processor = id, requestKey = key, buffer = receive.buffer, startTimeMs = time.milliseconds, remoteAddress = address);
            requestChannel.sendRequest(req);
            key.attach(null);
            // explicitly reset interest ops to not READ, no need to wake up the selector just yet;
            key.interestOps(key.interestOps & (~SelectionKey.OP_READ));
        } else {
            // more reading to be done;
            trace("Did not finish reading, registering for read again on connection " + socketChannel.socket.getRemoteSocketAddress())
            key.interestOps(SelectionKey.OP_READ);
            wakeup();
        }
    }

    /*
     * Process writes to ready sockets
     */
    public void write(SelectionKey key) {
        val socketChannel = channelFor(key);
        val response = key.attachment().asInstanceOf < RequestChannel.Response >
                val responseSend = response.responseSend;
        if (responseSend == null)
            throw new IllegalStateException("Registered for write interest but no response attached to key.")
        val written = responseSend.writeTo(socketChannel);
        trace(written + " bytes written to " + socketChannel.socket.getRemoteSocketAddress() + " using key " + key);
        if (responseSend.complete) {
            response.request.updateRequestMetrics();
            key.attach(null);
            trace("Finished writing, registering for read on connection " + socketChannel.socket.getRemoteSocketAddress())
            key.interestOps(SelectionKey.OP_READ);
        } else {
            trace("Did not finish writing, registering for write again on connection " + socketChannel.socket.getRemoteSocketAddress())
            key.interestOps(SelectionKey.OP_WRITE);
            wakeup();
        }
    }

    private SocketChannel channelFor(SelectionKey key) {
        return key.channel();
    }

    privatevoid maybeCloseOldestConnection

    {
        if (currentTimeNanos > nextIdleCloseCheckTime) {
            if (lruConnections.isEmpty) {
                nextIdleCloseCheckTime = currentTimeNanos + connectionsMaxIdleNanos;
            } else {
                val oldestConnectionEntry = lruConnections.entrySet.iterator().next();
                val connectionLastActiveTime = oldestConnectionEntry.getValue;
                nextIdleCloseCheckTime = connectionLastActiveTime + connectionsMaxIdleNanos;
                if (currentTimeNanos > nextIdleCloseCheckTime) {
                    val SelectionKey key = oldestConnectionEntry.getKey;
                    trace("About to close the idle connection from " + key.channel.asInstanceOf < SocketChannel >.socket.getRemoteSocketAddress;
                    +" due to being idle for " + (currentTimeNanos - connectionLastActiveTime) / 1000 / 1000 + " millis")
                    close(key);
                }
            }
        }
    }

}

    public void accept(SocketChannel socketChannel) {
        newConnections.add(socketChannel);
        wakeup();
    }


}
