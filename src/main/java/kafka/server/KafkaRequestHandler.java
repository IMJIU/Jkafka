package kafka.server;/**
 * Created by zhoulf on 2017/5/11.
 */

import com.yammer.metrics.core.Meter;
import kafka.network.RequestChannel;
import kafka.utils.Logging;
import kafka.utils.Time;

/**
 * A thread that answers kafka requests.
 */
public class KafkaRequestHandler extends Logging implements Runnable {
    public Integer id;
    public Integer brokerId;
    public Meter aggregateIdleMeter;
    public Integer totalHandlerThreads;
    public RequestChannel requestChannel;
    public KafkaApis apis;

    public KafkaRequestHandler(Integer id, Integer brokerId, Meter aggregateIdleMeter, Integer totalHandlerThreads, RequestChannel requestChannel, KafkaApis apis) {
        this.id = id;
        this.brokerId = brokerId;
        this.aggregateIdleMeter = aggregateIdleMeter;
        this.totalHandlerThreads = totalHandlerThreads;
        this.requestChannel = requestChannel;
        this.apis = apis;
        loggerName("<Kafka Request Handler " + id + " on Broker " + brokerId + ">, ");
    }

    public void run() {
        while (true) {
            try {
                RequestChannel.Request req = null;
                while (req == null) {
                    // We use a single meter for aggregate idle percentage for the thread pool.;
                    // Since meter is calculated as total_recorded_value / time_window and;
                    // time_window is independent of the number of threads, each recorded idle;
                    // time should be discounted by # threads.;
                    long startSelectTime = Time.get().nanoseconds();
                    req = requestChannel.receiveRequest(300L);
                    long idleTime = Time.get().nanoseconds() - startSelectTime;
                    aggregateIdleMeter.mark(idleTime / totalHandlerThreads);
                }

                if (req.equals(RequestChannel.AllDone)) {
                    debug(String.format("Kafka request handler %d on broker %d received shut down command", id, brokerId));
                    return;
                }
                req.requestDequeueTimeMs = Time.get().milliseconds();
                trace(String.format("Kafka request handler %d on broker %d handling request %s", id, brokerId, req));
                apis.handle(req);
            } catch (Throwable e) {
                error("Exception when handling request", e);
            }
        }
    }

    public void shutdown() throws InterruptedException {
        requestChannel.sendRequest(RequestChannel.AllDone);
    }

}
