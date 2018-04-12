package kafka.server;

/**
 * @author zhoulf
 * @create 2017-10-25 31 11
 **/

import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Metric;
import kafka.api.FetchResponse;
import kafka.api.FetchResponseSend;
import kafka.metrics.KafkaMetricsGroup;
import kafka.network.RequestChannel;

import java.util.concurrent.TimeUnit;

/**
 * The purgatory holding delayed fetch requests
 */
public class FetchRequestPurgatory extends RequestPurgatory<DelayedFetch> {
    ReplicaManager replicaManager;
    RequestChannel requestChannel;

    public FetchRequestPurgatory(ReplicaManager replicaManager, RequestChannel requestChannel) {
        super(replicaManager.config.brokerId, replicaManager.config.fetchPurgatoryPurgeIntervalRequests);
        this.replicaManager = replicaManager;
        this.requestChannel = requestChannel;
        this.logIdent = String.format("<FetchRequestPurgatory-%d> ", replicaManager.config.brokerId);
    }

    private class DelayedFetchRequestMetrics extends KafkaMetricsGroup {
        public boolean forFollower;
        private String metricPrefix;
        public Meter expiredRequestMeter;

        public DelayedFetchRequestMetrics(boolean forFollower) {
            this.forFollower = forFollower;
            if (forFollower) metricPrefix = "Follower";
            else metricPrefix = "Consumer";
            expiredRequestMeter = newMeter(metricPrefix + "ExpiresPerSecond", "requests", TimeUnit.SECONDS);
        }

    }

    private DelayedFetchRequestMetrics aggregateFollowerFetchRequestMetrics = new DelayedFetchRequestMetrics(true);
    private DelayedFetchRequestMetrics aggregateNonFollowerFetchRequestMetrics = new DelayedFetchRequestMetrics(false);

    private void recordDelayedFetchExpired(Boolean forFollower) {
        DelayedFetchRequestMetrics metrics;
        if (forFollower) metrics = aggregateFollowerFetchRequestMetrics;
        else metrics = aggregateNonFollowerFetchRequestMetrics;

        metrics.expiredRequestMeter.mark();
    }

    /**
     * Check if a specified delayed fetch request is satisfied
     */
    public Boolean checkSatisfied(DelayedFetch delayedFetch) {
        return delayedFetch.isSatisfied(replicaManager);
    }


    /**
     * When a delayed fetch request expires just answer it with whatever data is present
     */
    public void expire(DelayedFetch delayedFetch) {
        debug(String.format("Expiring fetch request %s.", delayedFetch.fetch));
        boolean fromFollower = delayedFetch.fetch.isFromFollower();
        recordDelayedFetchExpired(fromFollower);
        respond(delayedFetch);
    }

    // purgatory TODO should not be responsible for sending back the responses;
    public void respond(DelayedFetch delayedFetch) {
        FetchResponse response = delayedFetch.respond(replicaManager);
        requestChannel.sendResponse(new RequestChannel.Response(delayedFetch.request, new FetchResponseSend(response)));
    }
}
