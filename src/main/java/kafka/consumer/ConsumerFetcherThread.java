package kafka.consumer;

import kafka.api.FetchResponsePartitionData;
import kafka.api.OffsetRequest;
import kafka.api.Request;
import kafka.cluster.Broker;
import kafka.log.TopicAndPartition;
import kafka.message.ByteBufferMessageSet;
import kafka.server.AbstractFetcherThread;
import kafka.utils.Sc;

import java.util.Map;

/**
 * @author zhoulf
 * @create 2017-12-14 55 16
 **/

public class ConsumerFetcherThread extends AbstractFetcherThread {
    public String name;
    public ConsumerConfig config;
    public Broker sourceBroker;
    public Map<TopicAndPartition, PartitionTopicInfo> partitionMap;
    public ConsumerFetcherManager consumerFetcherManager;

    public ConsumerFetcherThread(String name, ConsumerConfig config, Broker sourceBroker, Map partitionMap, ConsumerFetcherManager consumerFetcherManager) {
        super(name, true, config.clientId, sourceBroker, config.socketTimeoutMs, config.socketReceiveBufferBytes, config.fetchMessageMaxBytes, Request.OrdinaryConsumerId, config.fetchWaitMaxMs, config.fetchMinBytes);
        this.name = name;
        this.config = config;
        this.sourceBroker = sourceBroker;
        this.partitionMap = partitionMap;
        this.consumerFetcherManager = consumerFetcherManager;
    }

    // process fetched data;
    public void processPartitionData(TopicAndPartition topicAndPartition, Long fetchOffset, FetchResponsePartitionData partitionData) {
        PartitionTopicInfo pti = partitionMap.get(topicAndPartition);
        if (!pti.getFetchOffset().equals(fetchOffset))
            throw new RuntimeException(String.format("Offset doesn't match for partition <%s,%d> pti offset: %d fetch offset: %d",
                    topicAndPartition.topic, topicAndPartition.partition, pti.getFetchOffset(), fetchOffset));
        pti.enqueue((ByteBufferMessageSet) partitionData.messages);
    }

    // handle a partition whose offset is out of range and return a new fetch offset;
    public Long handleOffsetOutOfRange(TopicAndPartition topicAndPartition) {
        Long startTimestamp = 0L;
        if (OffsetRequest.SmallestTimeString.equals(config.autoOffsetReset)) {
            startTimestamp = OffsetRequest.EarliestTime;
        } else if (OffsetRequest.LargestTimeString.equals(config.autoOffsetReset)) {
            startTimestamp = OffsetRequest.LatestTime;
        } else {
            startTimestamp = OffsetRequest.LatestTime;
        }
        Long newOffset = simpleConsumer.earliestOrLatestOffset(topicAndPartition, startTimestamp, Request.OrdinaryConsumerId);
        PartitionTopicInfo pti = partitionMap.get(topicAndPartition);
        pti.resetFetchOffset(newOffset);
        pti.resetConsumeOffset(newOffset);
        return newOffset;
    }

    // any logic for partitions whose leader has changed;
    public void handlePartitionsWithErrors(Iterable<TopicAndPartition> partitions) {
        removePartitions(Sc.toSet(partitions));
        consumerFetcherManager.addPartitionsWithError(partitions);
    }
}
