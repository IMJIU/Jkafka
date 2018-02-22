package kafka.server;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.yammer.metrics.core.Meter;
import kafka.api.RequestOrResponse;
import kafka.func.Handler;
import kafka.log.TopicAndPartition;
import kafka.metrics.KafkaMetricsGroup;
import kafka.network.BoundedByteBufferSend;
import kafka.network.RequestChannel;
import kafka.utils.Pool;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * The purgatory holding delayed producer requests
 * 炼狱控制延迟producer的请求
 */
public class ProducerRequestPurgatory extends RequestPurgatory<DelayedProduce> {

    public ReplicaManager replicaManager;
    public OffsetManager offsetManager;
    public RequestChannel requestChannel;


    public ProducerRequestPurgatory(ReplicaManager replicaManager, OffsetManager offsetManager, RequestChannel requestChannel) {
        super(replicaManager.config.brokerId, replicaManager.config.producerPurgatoryPurgeIntervalRequests);
        this.replicaManager = replicaManager;
        this.offsetManager = offsetManager;
        this.requestChannel = requestChannel;
        this.logIdent = String.format("<ProducerRequestPurgatory-%d> ", replicaManager.config.brokerId);
    }

    private class DelayedProducerRequestMetrics extends KafkaMetricsGroup {
        public Optional<TopicAndPartition> metricId;
        public Meter expiredRequestMeter;
        public Map<String, String> tags;

        public DelayedProducerRequestMetrics(Optional<TopicAndPartition> metricId) {
            this.metricId = metricId;
            if (metricId.isPresent()) {
                TopicAndPartition topicAndPartition = metricId.get();
                tags = ImmutableMap.of("topic", topicAndPartition.topic, "partition", topicAndPartition.partition.toString());
            } else {
                tags = Maps.newHashMap();
            }
            expiredRequestMeter = newMeter("ExpiresPerSecond", "requests", TimeUnit.SECONDS, tags);

        }
    }

    private Pool<TopicAndPartition, DelayedProducerRequestMetrics> producerRequestMetricsForKey() {
        Handler<TopicAndPartition, DelayedProducerRequestMetrics> valueFactory = k -> new DelayedProducerRequestMetrics(Optional.of(k));
        return new Pool<>(Optional.of(valueFactory));
    }

    private DelayedProducerRequestMetrics aggregateProduceRequestMetrics = new DelayedProducerRequestMetrics(Optional.empty());

    private void recordDelayedProducerKeyExpired(TopicAndPartition metricId) {
        DelayedProducerRequestMetrics keyMetrics = producerRequestMetricsForKey().getAndMaybePut(metricId);
        Lists.newArrayList(keyMetrics, aggregateProduceRequestMetrics).forEach(m -> m.expiredRequestMeter.mark());
    }

    /**
     * Check if a specified delayed fetch request is satisfied
     */
    public Boolean checkSatisfied(DelayedProduce delayedProduce) {
        return delayedProduce.isSatisfied(replicaManager);
    }

    /**
     * When a delayed produce request expires answer it with possible time out error codes
     */
    public void expire(DelayedProduce delayedProduce) {
        debug(String.format("Expiring produce request %s.", delayedProduce.produce));
        delayedProduce.partitionStatus.forEach((topicPartition, responseStatus) -> {
            if (responseStatus.acksPending) recordDelayedProducerKeyExpired(topicPartition);
        });
        respond(delayedProduce);
    }

    // purgatory TODO should not be responsible for sending back the responses;
    public void respond(DelayedProduce delayedProduce) {
        RequestOrResponse response = delayedProduce.respond(offsetManager);
        requestChannel.sendResponse(new RequestChannel.Response(delayedProduce.request, new BoundedByteBufferSend(response)));
    }
}
