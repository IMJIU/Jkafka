package kafka.network;

import com.google.common.collect.ImmutableMap;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import kafka.metrics.KafkaMetricsGroup;
import kafka.utils.Time;
import kafka.utils.Utils;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An NIO socket server. The threading model is
 * 1 Acceptor thread that handles new connections
 * N Processor threads that each have their own selector and read requests from sockets
 * M Handler threads that handle requests and produce responses back to the processor threads for writing.
 */
public class SocketServer extends KafkaMetricsGroup {
    Integer brokerId;
    String host;
    Integer port;
    Integer numProcessorThreads;
    Integer maxQueuedRequests;
    Integer sendBufferSize;
    Integer recvBufferSize;
    Integer maxRequestSize = Integer.MAX_VALUE;
    Integer maxConnectionsPerIp = Integer.MAX_VALUE;
    Long connectionsMaxIdleMs;
    Map<String, Integer> maxConnectionsPerIpOverrides;

    public SocketServer(Integer brokerId, String host, Integer port, Integer numProcessorThreads, Integer maxQueuedRequests, Integer sendBufferSize, Integer recvBufferSize, Integer maxRequestSize, Integer maxConnectionsPerIp, Long connectionsMaxIdleMs, Map<String, Integer> maxConnectionsPerIpOverrides) {
        this.brokerId = brokerId;
        this.host = host;
        this.port = port;
        this.numProcessorThreads = numProcessorThreads;
        this.maxQueuedRequests = maxQueuedRequests;
        this.sendBufferSize = sendBufferSize;
        this.recvBufferSize = recvBufferSize;
        this.maxRequestSize = maxRequestSize;
        this.maxConnectionsPerIp = maxConnectionsPerIp;
        this.connectionsMaxIdleMs = connectionsMaxIdleMs;
        this.maxConnectionsPerIpOverrides = maxConnectionsPerIpOverrides;
        loggerName("[Socket Server on Broker " + brokerId + "], ");
        processors =  new Processor[numProcessorThreads];
        requestChannel = new RequestChannel(numProcessorThreads, maxQueuedRequests);
    }

    private Time time = Time.get();
    private Processor[] processors;
    private volatile Acceptor acceptor = null;
    public RequestChannel requestChannel ;

    /* a meter to track the average free capacity of the network processors */
    private Meter aggregateIdleMeter = newMeter("NetworkProcessorAvgIdlePercent", "percent", TimeUnit.NANOSECONDS);

    /**
     * Start the socket server
     */
    public void startup() throws InterruptedException, IOException {
        ConnectionQuotas quotas = new ConnectionQuotas(maxConnectionsPerIp, maxConnectionsPerIpOverrides);
        for (int i = 0; i < numProcessorThreads; i++) {
            processors[i] = new Processor(i,
                    time,
                    maxRequestSize,
                    aggregateIdleMeter,
                    newMeter("IdlePercent", "percent", TimeUnit.NANOSECONDS, ImmutableMap.of("networkProcessor", "" + i)),
                    numProcessorThreads,
                    requestChannel,
                    quotas,
                    connectionsMaxIdleMs);
            Utils.newThread(String.format("kafka-network-thread-%d-%d", port, i), processors[i], false).start();
        }

        newGauge("ResponsesBeingSent", new Gauge<Object>() {
            @Override
            public Object value() {
                AtomicInteger total = new AtomicInteger();
                Arrays.stream(processors).forEach(p -> total.set(total.get() + p.countInterestOps(SelectionKey.OP_WRITE)));
                return total.intValue();
            }
        });

        // register the processor threads for notification of responses;
        requestChannel.addResponseListener(id -> processors[id].wakeup());

        // start accepting connections;
        this.acceptor = new Acceptor(host, port, processors, sendBufferSize, recvBufferSize, quotas);
        Utils.newThread("kafka-socket-acceptor", acceptor, false).start();
        acceptor.awaitStartup();
        info("Started");
    }

    /**
     * Shutdown the socket server
     */
    public void shutdown() throws InterruptedException {
        info("Shutting down");
        if (acceptor != null)
            acceptor.shutdown();
        for (Processor processor : processors)
            processor.shutdown();
        info("Shutdown completed");
    }
}


