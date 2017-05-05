package kafka.network;


import com.yammer.metrics.core.Meter;
import kafka.common.KafkaException;
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
    public Long connectionsMaxIdleMs;

    private ConcurrentLinkedQueue<SocketChannel> newConnections = new ConcurrentLinkedQueue<SocketChannel>();
    private Long connectionsMaxIdleNanos;
    private Long currentTimeNanos = System.nanoTime();
    private LinkedHashMap<SelectionKey, Long> lruConnections = new LinkedHashMap<>();
    private Long nextIdleCloseCheckTime;

    public Processor(Integer id, Time time, Integer maxRequestSize, Meter aggregateIdleMeter, Meter idleMeter, Integer totalProcessorThreads, RequestChannel requestChannel, ConnectionQuotas connectionQuotas, long connectionsMaxIdleMs) {
        super(connectionQuotas);
        this.id = id;
        this.time = time;
        this.maxRequestSize = maxRequestSize;
        this.aggregateIdleMeter = aggregateIdleMeter;
        this.idleMeter = idleMeter;
        this.totalProcessorThreads = totalProcessorThreads;
        this.requestChannel = requestChannel;
        this.connectionQuotas = connectionQuotas;
        this.connectionsMaxIdleMs = connectionsMaxIdleMs;
        connectionsMaxIdleNanos = connectionsMaxIdleMs * 1000 * 1000;
        nextIdleCloseCheckTime = currentTimeNanos + connectionsMaxIdleNanos;
    }


    public void run() {
        startupComplete();

        while (isRunning()) {
            try {
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
                            if (key.isReadable()) {
                                read(key);
                            } else if (key.isWritable()) {
                                write(key);
                            } else if (!key.isValid()) {
                                close(key);
                            } else {
                                throw new IllegalStateException("Unrecognized key state for processor thread.");
                            }
                        } catch (InvalidRequestException e) {
                            info(String.format("Closing socket connection to %s due to invalid request: %s", channelFor(key).socket().getInetAddress(), e.getMessage()));
                            close(key);
                        } catch (Exception e) {
                            error("Closing socket for " + channelFor(key).socket().getInetAddress() + " because of error", e);
                            close(key);
                        }
                    }
                }
                maybeCloseOldestConnection();
            } catch (IOException e) {
                error(e.getMessage(), e);
            }
        }
        debug("Closing selector.");
        try {
            closeAll();
        } catch (IOException e) {
            error(e.getMessage(), e);
        }
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
        RequestChannel.Response curr = requestChannel.receiveResponse(id);
        while (curr != null) {
            SelectionKey key = (SelectionKey) curr.request.requestKey;
            try {
                switch (curr.responseAction) {
                    case NoOpAction:
                        // There is no response to send to the client, we need to read more pipelined requests;
                        // that are sitting in the server's socket buffer;
                        curr.request.updateRequestMetrics();
                        trace("Socket server received empty response to send, registering for read: " + curr);
                        key.interestOps(SelectionKey.OP_READ);
                        key.attach(null);
                        break;
                    case SendAction:
                        trace("Socket server received response to send, registering for write: " + curr);
                        key.interestOps(SelectionKey.OP_WRITE);
                        key.attach(curr);
                        break;
                    case CloseConnectionAction:
                        curr.request.updateRequestMetrics();
                        trace("Closing socket connection actively according to the response code.");
                        close(key);
                        break;
                    default:
                        throw new KafkaException("No mapping found for response code ");
                }
            } catch (CancelledKeyException e) {
                debug("Ignoring response for closed socket.");
                close(key);
            } finally {
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
    private void configureNewConnections() throws ClosedChannelException {
        while (newConnections.size() > 0) {
            SocketChannel channel = newConnections.poll();
            debug("Processor " + id + " listening to new connection from " + channel.socket().getRemoteSocketAddress());
            channel.register(selector, SelectionKey.OP_READ);
        }
    }

    /**
     * Process reads from ready sockets
     */
    public void read(SelectionKey key) {
        try {
            lruConnections.put(key, currentTimeNanos);
            SocketChannel socketChannel = channelFor(key);
            Receive receive = (Receive) key.attachment();
            if (key.attachment() == null) {
                receive = new BoundedByteBufferReceive(maxRequestSize);
                key.attach(receive);
            }
            Integer read = receive.readFrom(socketChannel);
            SocketAddress address = socketChannel.socket().getRemoteSocketAddress();
            trace(read + " bytes read from " + address);
            if (read < 0) {
                close(key);
            } else if (receive.complete()) {
                RequestChannel.Request req = new RequestChannel.Request(id, key, receive.buffer(), time.milliseconds(), address);
                requestChannel.sendRequest(req);
                key.attach(null);
                // explicitly reset interest ops to not READ, no need to wake up the selector just yet;
                key.interestOps(key.interestOps() & (~SelectionKey.OP_READ));
            } else {
                // more reading to be done;
                trace("Did not finish reading, registering for read again on connection " + socketChannel.socket().getRemoteSocketAddress());
                key.interestOps(SelectionKey.OP_READ);
                wakeup();
            }
        } catch (InterruptedException e) {
            error(e.getMessage(), e);
        }
    }


    /*
     * Process writes to ready sockets
     */
    public void write(SelectionKey key) {
        SocketChannel socketChannel = channelFor(key);
        RequestChannel.Response response = (RequestChannel.Response) key.attachment();
        Send responseSend = response.responseSend;
        if (responseSend == null)
            throw new IllegalStateException("Registered for write interest but no response attached to key.");
        Integer written = responseSend.writeTo(socketChannel);
        trace(written + " bytes written to " + socketChannel.socket().getRemoteSocketAddress() + " using key " + key);
        if (responseSend.complete()) {
            response.request.updateRequestMetrics();
            key.attach(null);
            trace("Finished writing, registering for read on connection " + socketChannel.socket().getRemoteSocketAddress());
            key.interestOps(SelectionKey.OP_READ);
        } else {
            trace("Did not finish writing, registering for write again on connection " + socketChannel.socket().getRemoteSocketAddress());
            key.interestOps(SelectionKey.OP_WRITE);
            wakeup();
        }
    }

    private SocketChannel channelFor(SelectionKey key) {
        return (SocketChannel) key.channel();
    }

    private void maybeCloseOldestConnection() {
        if (currentTimeNanos > nextIdleCloseCheckTime) {
            if (lruConnections.isEmpty()) {
                nextIdleCloseCheckTime = currentTimeNanos + connectionsMaxIdleNanos;
            } else {
                Map.Entry<SelectionKey, Long> oldestConnectionEntry = lruConnections.entrySet().iterator().next();
                Long connectionLastActiveTime = oldestConnectionEntry.getValue();
                nextIdleCloseCheckTime = connectionLastActiveTime + connectionsMaxIdleNanos;
                if (currentTimeNanos > nextIdleCloseCheckTime) {
                    SelectionKey key = oldestConnectionEntry.getKey();
                    trace("About to close the idle connection from " + ((SocketChannel) key.channel()).socket().getRemoteSocketAddress()
                            + " due to being idle for " + (currentTimeNanos - connectionLastActiveTime) / 1000 / 1000 + " millis");
                    close(key);
                }
            }
        }
    }
}
