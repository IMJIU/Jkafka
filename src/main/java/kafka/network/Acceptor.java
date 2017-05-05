package kafka.network;

import kafka.common.KafkaException;
import kafka.func.Handler;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;


public class Acceptor extends AbstractServerThread {
    public String host;
    public Integer port;
    private Processor[] processors;
    public Integer sendBufferSize;
    public Integer recvBufferSize;

    public Acceptor(String host, Integer port, Processor[] processors, Integer sendBufferSize, Integer recvBufferSize, ConnectionQuotas connectionQuotas) throws IOException {
        super(connectionQuotas);
        this.host = host;
        this.port = port;
        this.processors = processors;
        this.sendBufferSize = sendBufferSize;
        this.recvBufferSize = recvBufferSize;
        this.connectionQuotas = connectionQuotas;
        init();
    }

    public Acceptor(ConnectionQuotas connectionQuotas) throws IOException {
        super(connectionQuotas);
        this.connectionQuotas = connectionQuotas;
        init();
    }

    public void init() throws IOException {
        serverChannel = openServerSocket(host, port);
    }

    public ServerSocketChannel serverChannel;

    /**
     * Accept loop that checks for new connection attempts
     */
    @Override
    public void run() {
        try {
            serverChannel.register(selector, SelectionKey.OP_ACCEPT);
            startupComplete();
            Integer currentProcessor = 0;
            while (isRunning()) {
                Integer ready = selector.select(500);
                if (ready > 0) {
                    Set<SelectionKey> keys = selector.selectedKeys();
                    Iterator<SelectionKey> iter = keys.iterator();
                    while (iter.hasNext() && isRunning()) {
                        SelectionKey key;
                        try {
                            key = iter.next();
                            iter.remove();
                            if (key.isAcceptable())
                                accept(key, processors[currentProcessor]);
                            else
                                throw new IllegalStateException("Unrecognized key state for acceptor thread.");

                            // round robin to the next processor thread;
                            currentProcessor = (currentProcessor + 1) % processors.length;
                        } catch (Exception e) {
                            error("Error while accepting connection", e);
                        }
                    }
                }
            }
            debug("Closing server socket and selector.");
            swallowError(() -> serverChannel.close());
            swallowError(() -> selector.close());
            shutdownComplete();
        } catch (ClosedChannelException e) {
            error(e.getMessage(),e);
        } catch (IOException e) {
            error(e.getMessage(),e);
        }

    }

    /*
     * Create a server socket to listen for connections on.
     */
    public ServerSocketChannel openServerSocket(String host, Integer port) throws IOException {
        InetSocketAddress socketAddress;
        if (host == null || host.trim().isEmpty())
            socketAddress = new InetSocketAddress(port);
        else
            socketAddress = new InetSocketAddress(host, port);
        ServerSocketChannel serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        serverChannel.socket().setReceiveBufferSize(recvBufferSize);
        try {
            serverChannel.socket().bind(socketAddress);
            info(String.format("Awaiting socket connections on %s:%d.", socketAddress.getHostName(), port));
        } catch (SocketException e) {
            throw new KafkaException(String.format("Socket server failed to bind to %s:%d: %s.", socketAddress.getHostName(), port, e.getMessage()), e);
        }
        return serverChannel;
    }

    /*
     * Accept a new connection
     */
    public void accept(SelectionKey key, Processor processor) throws IOException {
        ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
        SocketChannel socketChannel = serverSocketChannel.accept();
        try {
            connectionQuotas.inc(socketChannel.socket().getInetAddress());
            socketChannel.configureBlocking(false);
            socketChannel.socket().setTcpNoDelay(true);
            socketChannel.socket().setSendBufferSize(sendBufferSize);

            debug(String.format("Accepted connection from %s on %s. sendBufferSize <actual|requested]: [%d|%d> recvBufferSize <actual|requested]: [%d|%d>",
                    socketChannel.socket().getInetAddress(), socketChannel.socket().getLocalSocketAddress(),
                    socketChannel.socket().getSendBufferSize(), sendBufferSize,
                    socketChannel.socket().getReceiveBufferSize(), recvBufferSize));

            processor.accept(socketChannel);
        } catch (TooManyConnectionsException e) {
            info(String.format("Rejected connection from %s, address already has the configured maximum of %d connections.", e.ip, e.count));
            close(socketChannel);
        }
    }

}
