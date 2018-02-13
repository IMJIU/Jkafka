package kafka.network;


import kafka.api.RequestOrResponse;
import kafka.utils.Logging;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.*;

/**
 * A simple blocking channel with timeouts correctly enabled.
 */
//@nonthreadsafe
public class BlockingChannel extends Logging {
    public static final int UseDefaultBufferSize = -1;
    public String host;
    public Integer port;
    public Integer readBufferSize;
    public Integer writeBufferSize;
    public Integer readTimeoutMs;

    public BlockingChannel(String host, Integer port, Integer readBufferSize, Integer writeBufferSize, Integer readTimeoutMs) {
        this.host = host;
        this.port = port;
        this.readBufferSize = readBufferSize;
        this.writeBufferSize = writeBufferSize;
        this.readTimeoutMs = readTimeoutMs;
        this.connectTimeoutMs = readTimeoutMs;
    }

    private boolean connected = false;
    private SocketChannel channel = null;
    private ReadableByteChannel readChannel = null;
    private GatheringByteChannel writeChannel = null;
    private Object lock = new Object();
    private Integer connectTimeoutMs = readTimeoutMs;

    public void connect() {
        synchronized (lock) {
            if (!connected) {
                try {
                    channel = SocketChannel.open();
                    if (readBufferSize > 0)
                        channel.socket().setReceiveBufferSize(readBufferSize);
                    if (writeBufferSize > 0)
                        channel.socket().setSendBufferSize(writeBufferSize);
                    channel.configureBlocking(true);
                    channel.socket().setSoTimeout(readTimeoutMs);
                    channel.socket().setKeepAlive(true);
                    channel.socket().setTcpNoDelay(true);
                    channel.socket().connect(new InetSocketAddress(host, port), connectTimeoutMs);

                    writeChannel = channel;
                    readChannel = Channels.newChannel(channel.socket().getInputStream());
                    connected = true;
                    // settings may not match what we requested above;
                    String msg = "Created socket with SO_TIMEOUT = %d (requested %d), SO_RCVBUF = %d (requested %d), SO_SNDBUF = %d (requested %d), connectTimeoutMs = %d.";
                    debug(String.format(msg, channel.socket().getSoTimeout(),
                            readTimeoutMs,
                            channel.socket().getReceiveBufferSize(),
                            readBufferSize,
                            channel.socket().getSendBufferSize(),
                            writeBufferSize,
                            connectTimeoutMs));
                } catch (Throwable e) {
//                    logger.error(e.getMessage(), e);
                    disconnect();
                }
            }
        }
    }

    public void disconnect() {
        synchronized (lock) {
            if (channel != null) {
                swallow(() -> channel.close());
                swallow(() -> channel.socket().close());
                channel = null;
                writeChannel = null;
            }
            // closing the main socket channel *should* close the read channel;
            // but let's do it to be sure.;
            if (readChannel != null) {
                swallow(() -> readChannel.close());
                readChannel = null;
            }
            connected = false;
            logger.error("=============why close!!!");
        }
    }

    public boolean isConnected() {
        return connected;
    }

    public Integer send(RequestOrResponse request) throws IOException {
        if (!connected)
            throw new ClosedChannelException();

        BoundedByteBufferSend send = new BoundedByteBufferSend(request);
        return send.writeCompletely(writeChannel);
    }

    public Receive receive() throws IOException {
        if (!connected)
            throw new ClosedChannelException();

        BoundedByteBufferReceive response = new BoundedByteBufferReceive();
        response.readCompletely(readChannel);
        return response;
    }

}
