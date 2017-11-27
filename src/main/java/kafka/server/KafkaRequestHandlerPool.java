package kafka.server;

import com.yammer.metrics.core.Meter;
import kafka.metrics.KafkaMetricsGroup;
import kafka.network.RequestChannel;
import kafka.utils.Utils;

import java.util.concurrent.TimeUnit;

/**
 * @author zhoulf
 * @create 2017-11-27 02 18
 **/

class KafkaRequestHandlerPool extends KafkaMetricsGroup {
    public Integer brokerId;
    public RequestChannel requestChannel;
    public KafkaApis apis;
    public Integer numThreads;
    public Thread[] threads;
    public KafkaRequestHandler[] runnables;

    public KafkaRequestHandlerPool(Integer brokerId, RequestChannel requestChannel, KafkaApis apis, Integer numThreads) {
        this.brokerId = brokerId;
        this.requestChannel = requestChannel;
        this.apis = apis;
        this.numThreads = numThreads;
        this.logIdent = "<Kafka Request Handler on Broker " + brokerId + ">, ";
        threads = new Thread[numThreads];
        runnables = new KafkaRequestHandler[numThreads];
        for (int i = 0; i < numThreads; i++) {
            runnables[i] = new KafkaRequestHandler(i, brokerId, aggregateIdleMeter, numThreads, requestChannel, apis);
            threads[i] = Utils.daemonThread("kafka-request-handler-" + i, runnables[i]);
            threads[i].start();
        }
    }

    /* a meter to track the average free capacity of the request handlers */
    private Meter aggregateIdleMeter = newMeter("RequestHandlerAvgIdlePercent", "percent", TimeUnit.NANOSECONDS);


    public void shutdown() {
        info("shutting down");
        try {
            for (KafkaRequestHandler handler : runnables)
                handler.shutdown();
            for (Thread thread : threads)
                thread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        info("shut down completely");
    }
}
