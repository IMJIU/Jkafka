package kafka.producer;

import com.google.common.collect.Sets;
import kafka.api.PartitionMetadata;
import kafka.api.TopicMetadata;
import kafka.api.TopicMetadataResponse;
import kafka.client.ClientUtils;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.KafkaException;
import kafka.utils.Logging;
import kafka.utils.Sc;

import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * @author zhoulf
 * @create 2017-12-06 16 9
 **/


public class BrokerPartitionInfo extends Logging {
    public ProducerConfig producerConfig;
    public ProducerPool producerPool;
    public HashMap<String, TopicMetadata> topicPartitionInfo;
    public String brokerList;
    public List<Broker> brokers;

    public BrokerPartitionInfo(ProducerConfig producerConfig, ProducerPool producerPool, HashMap<String, TopicMetadata> topicPartitionInfo) {
        this.producerConfig = producerConfig;
        this.producerPool = producerPool;
        this.topicPartitionInfo = topicPartitionInfo;
        brokerList = producerConfig.brokerList;
        brokers = ClientUtils.parseBrokerList(brokerList);
    }

    /**
     * Return a sequence of (brokerId, numPartitions).
     *
     * @param topic the topic for which this information is to be returned
     * @return a sequence of (brokerId, numPartitions). Returns a zero-length
     * sequence if no brokers are available.
     */
    public List<PartitionAndLeader> getBrokerPartitionInfo(String topic, Integer correlationId) {
        debug(String.format("Getting broker partition info for topic %s", topic));
        // check if the cache has metadata for this topic;
        TopicMetadata topicMetadata = topicPartitionInfo.get(topic);
        TopicMetadata metadata = null;
        if (topicMetadata == null) {
            // refresh the topic metadata cache;
            updateInfo(Sets.newHashSet(topic), correlationId);
            topicMetadata = topicPartitionInfo.get(topic);
            if (topicMetadata == null) {
                throw new KafkaException("Failed to fetch topic metadata for topic: " + topic);
            }
        }
        List<PartitionMetadata> partitionMetadata = metadata.partitionsMetadata;
        if (partitionMetadata.size() == 0) {
            if (metadata.errorCode != ErrorMapping.NoError) {
                throw new KafkaException(ErrorMapping.exceptionFor(metadata.errorCode));
            } else {
                throw new KafkaException(String.format("Topic metadata %s has empty partition metadata and no error code", metadata));
            }
        }
        return Sc.map(partitionMetadata, m -> {
            if (m.leader != null) {
                Broker leader = m.leader;
                debug(String.format("Partition <%s,%d> has leader %d", topic, m.partitionId, leader.id));
                return new PartitionAndLeader(topic, m.partitionId, Optional.of(leader.id));
            } else {
                debug(String.format("Partition <%s,%d> does not have a leader yet", topic, m.partitionId));
                return new PartitionAndLeader(topic, m.partitionId, Optional.empty());
            }
        });
        //.sortWith((s, t) -> s.partitionId < t.partitionId);
    }

    /**
     * It updates the cache by issuing a get topic metadata request to a random broker.
     *
     * @param topics the topics for which the metadata is to be fetched
     */
    public void updateInfo(Set<String> topics, Integer correlationId) {
        List<TopicMetadata> topicsMetadata = null;
        TopicMetadataResponse topicMetadataResponse = ClientUtils.fetchTopicMetadata(topics, brokers, producerConfig, correlationId);
        topicsMetadata = topicMetadataResponse.topicsMetadata;
        // throw partition specific exception;
        topicsMetadata.forEach(tmd -> {
            trace(String.format("Metadata for topic %s is %s", tmd.topic, tmd));
            if (tmd.errorCode == ErrorMapping.NoError) {
                topicPartitionInfo.put(tmd.topic, tmd);
            } else
                warn(String.format("Error while fetching metadata <%s] for topic [%s>: %s ", tmd, tmd.topic, ErrorMapping.exceptionFor(tmd.errorCode).getClass()));
            tmd.partitionsMetadata.forEach(pmd -> {
                if (pmd.errorCode != ErrorMapping.NoError && pmd.errorCode == ErrorMapping.LeaderNotAvailableCode) {
                    warn(String.format("Error while fetching metadata %s for topic partition <%s,%d]: [%s>", pmd, tmd.topic, pmd.partitionId,
                            ErrorMapping.exceptionFor(pmd.errorCode).getClass()));
                } // any other error code (e.g. ReplicaNotAvailable) can be ignored since the producer does not need to access the replica and isr metadata;
            });
        });
        producerPool.updateProducer(topicsMetadata);
    }

}



