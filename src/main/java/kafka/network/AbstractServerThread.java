package kafka.network;/**
 * Created by zhoulf on 2017/4/20.
 */

import com.google.common.collect.Maps;
import kafka.common.KafkaException;
import kafka.utils.Logging;
import kafka.utils.Utils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A base class with some helper variables and methods
 */
public abstract class AbstractServerThread extends Logging implements Runnable {
    public ConnectionQuotas connectionQuotas;
    protected Selector selector;
    private CountDownLatch startupLatch = new CountDownLatch(1);
    private CountDownLatch shutdownLatch = new CountDownLatch(1);
    private AtomicBoolean alive = new AtomicBoolean(true);


    public AbstractServerThread(ConnectionQuotas connectionQuotas) {
        this.connectionQuotas = connectionQuotas;
        try {
            selector = Selector.open();
        } catch (IOException e) {
            error(e.getMessage(), e);
        }
    }

    /**
     * Initiates a graceful shutdown by signaling to stop and waiting for the shutdown to complete
     */
    public void shutdown() throws InterruptedException {
        alive.set(false);
        selector.wakeup();
        shutdownLatch.await();
    }

    /**
     * Wait for the thread to completely start up
     */
    public void awaitStartup() throws InterruptedException {
        startupLatch.await();
    }

    /**
     * Record that the thread startup is complete
     */
    protected void startupComplete() {
        startupLatch.countDown();
    }

    /**
     * Record that the thread shutdown is complete
     */
    protected void shutdownComplete() {
        shutdownLatch.countDown();
    }

    /**
     * Is the server still running?
     */
    protected Boolean isRunning() {
        return alive.get();
    }

    /**
     * Wakeup the thread for selection.
     */
    public void wakeup() {
        selector.wakeup();
    }

    /**
     * Close the given key and associated socket
     */
    public void close(SelectionKey key) {
        if (key != null) {
            key.attach(null);
            close((SocketChannel) key.channel());
            swallowError(() -> key.cancel());
        }
    }

    public void close(SocketChannel channel) {
        if (channel != null) {
            debug("Closing connection from " + channel.socket().getRemoteSocketAddress());
            connectionQuotas.dec(channel.socket().getInetAddress());
            swallowError(() -> channel.socket().close());
            swallowError(() -> channel.close());
        }
    }

    /**
     * Close all open connections
     */
    public void closeAll() throws IOException {
        // removes cancelled keys from selector.keys set;
        this.selector.selectNow();
        Iterator<SelectionKey> iter = this.selector.keys().iterator();
        while (iter.hasNext()) {
            SelectionKey key = iter.next();
            close(key);
        }
    }

    public Integer countInterestOps(Integer ops) {
        Integer count = 0;
        Iterator<SelectionKey> it = this.selector.keys().iterator();
        while (it.hasNext()) {
            if ((it.next().interestOps() & ops) != 0) {
                count += 1;
            }
        }
        return count;
    }
}

class ConnectionQuotas extends Logging{
    public Integer defaultMax;
    public Map<String, Integer> overrideQuotas;
    private Map<InetAddress, Integer> overrides;
    private Map<InetAddress, Integer> counts = Maps.newHashMap();

    public ConnectionQuotas(Integer defaultMax, Map<String, Integer> overrideQuotas) {
        this.defaultMax = defaultMax;
        this.overrideQuotas = overrideQuotas;
        overrides = Utils.mapKey(overrideQuotas, k -> {
            try {
                return InetAddress.getByName(k);
            } catch (UnknownHostException e) {
                error(e.getMessage(),e);
            }
            return null;
        });
    }

    public void inc(InetAddress addr) {
        synchronized (counts) {
            Integer count = counts.getOrDefault(addr, 0);
            counts.put(addr, count + 1);
            Integer max = overrides.getOrDefault(addr, defaultMax);
            System.out.println(count+"-"+max);
            if (count >= max)
                throw new TooManyConnectionsException(addr, max);
        }
    }

    public void dec(InetAddress addr) {
        synchronized (counts) {
            Integer count = counts.get(addr);
            if (count == 1)
                counts.remove(addr);
            else
                counts.put(addr, count - 1);
        }
    }

}

class TooManyConnectionsException extends KafkaException {
    public InetAddress ip;
    public Integer count;

    public TooManyConnectionsException(InetAddress ip, Integer count) {
        super(String.format("Too many connections from %s (maximum = %d)", ip, count));
        this.ip = ip;
        this.count = count;
    }
}