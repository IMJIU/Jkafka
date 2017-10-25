package kafka.server;

import com.google.common.collect.ImmutableMap;
import kafka.admin.AdminUtils;
import kafka.api.FetchResponsePartitionData;
import kafka.api.OffsetRequest;
import kafka.cluster.Broker;
import kafka.cluster.Replica;
import kafka.common.KafkaStorageException;
import kafka.log.LogConfig;
import kafka.log.TopicAndPartition;
import kafka.message.ByteBufferMessageSet;

/**
 * @author zhoulf
 * @create 2017-10-17 48 12
 **/

public class ReplicaFetcherThread extends AbstractFetcherThread {
    public String name;
    public Broker sourceBroker;
    public KafkaConfig brokerConfig;
    public ReplicaManager replicaMgr;

    public ReplicaFetcherThread(String name,  Broker sourceBroker, KafkaConfig brokerConfig, ReplicaManager replicaMgr) {
        super(name,
                false,
                name,
                sourceBroker,
                brokerConfig.replicaSocketTimeoutMs,
                brokerConfig.replicaSocketReceiveBufferBytes,
                brokerConfig.replicaFetchMaxBytes,
                brokerConfig.brokerId,
                brokerConfig.replicaFetchWaitMaxMs,
                brokerConfig.replicaFetchMinBytes);
        this.name = name;
        this.sourceBroker = sourceBroker;
        this.brokerConfig = brokerConfig;
        this.replicaMgr = replicaMgr;
    }

    // process fetched data;
    public void processPartitionData(TopicAndPartition topicAndPartition, Long fetchOffset, FetchResponsePartitionData partitionData) {
        try {
            String topic = topicAndPartition.topic;
            Integer partitionId = topicAndPartition.partition;
            Replica replica = replicaMgr.getReplica(topic, partitionId).get();
            ByteBufferMessageSet messageSet = (ByteBufferMessageSet) partitionData.messages;

            if (fetchOffset != replica.logEndOffset().messageOffset)
                throw new RuntimeException(String.format("Offset fetched mismatch offset = %d, log end offset = %d.", fetchOffset, replica.logEndOffset().messageOffset));
            logger.trace(String.format("Follower %d has replica log end offset %d for partition %s. Received %d messages and leader hw %d",
                    replica.brokerId, replica.logEndOffset().messageOffset, topicAndPartition, messageSet.sizeInBytes(), partitionData.hw));
            replica.log.get().append(messageSet, false);
            logger.trace(String.format("Follower %d has replica log end offset %d after appending %d bytes of messages for partition %s", replica.brokerId, replica.logEndOffset().messageOffset, messageSet.sizeInBytes(), topicAndPartition));
            Long followerHighWatermark = Long.min(replica.logEndOffset().messageOffset, partitionData.hw);
            // for the follower replica, we do not need to keep;
            // its segment base offset the physical position,
            // these values will be computed upon making the leader;
            replica.highWatermark_(new LogOffsetMetadata(followerHighWatermark));
            logger.trace(String.format("Follower %d set replica high watermark for partition <%s,%d> to %s", replica.brokerId, topic, partitionId, followerHighWatermark));
        } catch (KafkaStorageException e) {
            logger.error("Disk error while replicating data.", e);
            Runtime.getRuntime().halt(1);
        }
    }

    /**
     * Handle a partition whose offset is out of range and return a new fetch offset.
     */
    public Long handleOffsetOutOfRange(TopicAndPartition topicAndPartition) {
        Replica replica = replicaMgr.getReplica(topicAndPartition.topic, topicAndPartition.partition).get();

        /**
         * Unclean leader A election follower goes down, in the meanwhile the leader keeps appending messages. The follower comes back up
         * and before it has completely caught up with the leader's logs, all replicas in the ISR go down. The follower is now uncleanly
         * elected as the new leader, and it starts appending messages from the client. The old leader comes back up, becomes a follower
         * and it may discover that the current leader's end offset is behind its own end offset.
         *
         * In such a case, truncate the current follower's log to the current leader's end offset and continue fetching.
         *
         * There is a potential for a mismatch between the logs of the two replicas here. We don't fix this mismatch as of now.
         */
        val leaderEndOffset = simpleConsumer.earliestOrLatestOffset(topicAndPartition, OffsetRequest.LatestTime, brokerConfig.brokerId);
        if (leaderEndOffset < replica.logEndOffset().messageOffset) {
            // Prior to truncating the follower's log, ensure that doing so is not disallowed by the configuration for unclean leader election.;
            // This situation could only happen if the unclean election configuration for a topic changes while a replica is down. Otherwise,
            // we should never encounter this situation since a non-ISR leader cannot be elected if disallowed by the broker configuration.;
            if (!LogConfig.fromProps(brokerConfig.props.props, AdminUtils.fetchTopicConfig(replicaMgr.zkClient,
                    topicAndPartition.topic)).uncleanLeaderElectionEnable) {
                // Log a fatal error and shutdown the broker to ensure that data loss does not unexpectedly occur.;
                logger.error(String.format("Halting because log truncation is not allowed for topic %s,", topicAndPartition.topic) +
                        String.format(" Current leader %d's latest offset %d is less than replica %d's latest offset %d",
                                sourceBroker.id, leaderEndOffset, brokerConfig.brokerId, replica.logEndOffset().messageOffset));
                Runtime.getRuntime().halt(1);
            }

            replicaMgr.logManager.truncateTo(ImmutableMap.of(topicAndPartition, leaderEndOffset));
            logger.warn(String.format("Replica %d for partition %s reset its fetch offset from %d to current leader %d's latest offset %d",
                    brokerConfig.brokerId, topicAndPartition, replica.logEndOffset().messageOffset, sourceBroker.id, leaderEndOffset))
            return leaderEndOffset;
        } else {
            /**
             * The follower could have been down for a long time and when it starts up, its end offset could be smaller than the leader's
             * start offset because the leader has deleted old logs (log.logEndOffset < leaderStartOffset).
             *
             * Roll out a new log at the follower with the start offset equal to the current leader's start offset and continue fetching.
             */
            val leaderStartOffset = simpleConsumer.earliestOrLatestOffset(topicAndPartition, OffsetRequest.EarliestTime, brokerConfig.brokerId);
            replicaMgr.logManager.truncateFullyAndStartAt(topicAndPartition, leaderStartOffset);
            logger.warn(String.format("Replica %d for partition %s reset its fetch offset from %d to current leader %d's start offset %d",
                    brokerConfig.brokerId, topicAndPartition, replica.logEndOffset().messageOffset, sourceBroker.id, leaderStartOffset));
            return leaderStartOffset;
        }
    }

    // any logic for partitions whose leader has changed;
    public void handlePartitionsWithErrors(Iterable<TopicAndPartition> partitions) {
        // no handler needed since the controller will make the changes accordingly;
    }
}
