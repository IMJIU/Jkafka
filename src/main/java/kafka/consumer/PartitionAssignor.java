package kafka.consumer;


import com.google.common.collect.Maps;
import kafka.log.TopicAndPartition;
import kafka.utils.*;
import org.I0Itec.zkclient.ZkClient;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface PartitionAssignor {

    /**
     * Assigns partitions to consumer instances in a group.
     *
     * @return An assignment map of partition to consumer thread. This only includes assignments for threads that belong
     * to the given assignment-context's consumer.
     */
    Map<TopicAndPartition, ConsumerThreadId> assign(AssignmentContext ctx);

    static PartitionAssignor createInstance(String assignmentStrategy) {
        if ("roundrobin".equals(assignmentStrategy)) {
            return new RoundRobinAssignor();
        }
        return new RangeAssignor();
    }

}


class AssignmentContext {
    String group;
    String consumerId;

    public AssignmentContext(String group, String consumerId, Boolean excludeInternalTopics, ZkClient zkClient) {
        this.group = group;
        this.consumerId = consumerId;
        this.excludeInternalTopics = excludeInternalTopics;
        this.zkClient = zkClient;
    }

    Boolean excludeInternalTopics;
    ZkClient zkClient;

    public Map<String, Set<ConsumerThreadId>> myTopicThreadIds() {
        TopicCount myTopicCount = TopicCount.constructTopicCount(group, consumerId, zkClient, excludeInternalTopics);
        return myTopicCount.getConsumerThreadIdsPerTopic();
    }

    Map<String, List<Integer>> partitionsForTopic = ZkUtils.getPartitionsForTopics(zkClient, Sc.toList(myTopicThreadIds().keySet()));

    Map<String, List<ConsumerThreadId>> consumersForTopic =
            ZkUtils.getConsumersPerTopic(zkClient, group, excludeInternalTopics);

    List<String> consumers = ZkUtils.getConsumersInGroup(zkClient, group).sorted;
}

/**
 * The round-robin partition assignor lays out all the available partitions and all the available consumer threads. It
 * then proceeds to do a round-robin assignment from partition to consumer thread. If the subscriptions of all consumer
 * instances are identical, then the partitions will be uniformly distributed. (i.e., the partition ownership counts
 * will be within a delta of exactly one across all consumer threads.)
 * <p>
 * (For simplicity of implementation) the assignor is allowed to assign a given topic-partition to any consumer instance
 * and thread-id within that instance. Therefore, round-robin assignment is allowed only if:
 * a) Every topic has the same number of streams within a consumer instance
 * b) The set of subscribed topics is identical for every consumer instance within the group.
 */

class RoundRobinAssignor extends Logging implements PartitionAssignor {

    public Map<TopicAndPartition, ConsumerThreadId> assign(AssignmentContext ctx) {
        Map<TopicAndPartition, ConsumerThreadId> partitionOwnershipDecision = Maps.newHashMap();

        // check conditions (a) and (b);
        String headTopic = Sc.head(ctx.consumersForTopic).v1;
        Set<ConsumerThreadId> headThreadIdSet = Sc.toSet(Sc.head(ctx.consumersForTopic).v2);
        ctx.consumersForTopic.forEach((topic, threadIds) -> {
            Set<ConsumerThreadId> threadIdSet = Sc.toSet(threadIds);
            Prediction.require(threadIdSet == headThreadIdSet,
                    "Round-robin assignment is allowed only if all consumers in the group subscribe to the same topics, " +
                            "AND if the stream counts across topics are identical for a given consumer instance.\n" +
                            String.format("Topic %s has the following available consumer streams: %s\n", topic, threadIdSet) +
                            String.format("Topic %s has the following available consumer streams: %s\n", headTopic, headThreadIdSet));
        });

        val threadAssignor = Utils.circularIterator(Sc.toList(headThreadIdSet).sorted);

        info("Starting round-robin assignment with consumers " + ctx.consumers);
        List<TopicAndPartition> allTopicPartitions = Sc.sortWith(Sc.flatMap(ctx.partitionsForTopic, (topic, partitions) -> {
            info(String.format("Consumer %s rebalancing the following partitions for topic %s: %s",
                    ctx.consumerId, topic, partitions));
            return Sc.map(partitions, partition -> new TopicAndPartition(topic, partition)).stream();
        }), (topicPartition1, topicPartition2) ->
                topicPartition1.toString().hashCode() < topicPartition2.toString().hashCode());
        /*
       * Randomize the order by taking the hashcode to reduce the likelihood of all partitions of a given topic ending
       * up on one consumer (if it has a high enough stream count).
       */

        allTopicPartitions.forEach(topicPartition -> {
            ConsumerThreadId threadId = threadAssignor.next();
            if (threadId.consumer == ctx.consumerId)
                partitionOwnershipDecision.put(topicPartition, threadId);
        });

        return partitionOwnershipDecision;
    }
}

/**
 * Range partitioning works on a per-topic basis. For each topic, we lay out the available partitions in numeric order
 * and the consumer threads in lexicographic order. We then divide the number of partitions by the total number of
 * consumer streams (threads) to determine the number of partitions to assign to each consumer. If it does not evenly
 * divide, then the first few consumers will have one extra partition. For example, suppose there are two consumers C1
 * and C2 with two streams each, and there are five available partitions (p0, p1, p2, p3, p4). So each consumer thread
 * will get at least one partition and the first consumer thread will get one extra partition. So the assignment will be:
 * p0 -> C1-0, p1 -> C1-0, p2 -> C1-1, p3 -> C2-0, p4 -> C2-1
 */
class RangeAssignor extends Logging implements PartitionAssignor {

    public Map<TopicAndPartition, ConsumerThreadId> assign(AssignmentContext ctx) {
        Map<TopicAndPartition, ConsumerThreadId> partitionOwnershipDecision = Maps.newHashMap();
        Sc.foreach(ctx.myTopicThreadIds(), (topic, consumerThreadIdSet) -> {
            List<ConsumerThreadId> curConsumers = ctx.consumersForTopic.get(topic);
            List<Integer> curPartitions = ctx.partitionsForTopic.get(topic);

            Integer nPartsPerConsumer = curPartitions.size() / curConsumers.size();
            Integer nConsumersWithExtraPart = curPartitions.size() % curConsumers.size();

            info("Consumer " + ctx.consumerId + " rebalancing the following partitions: " + curPartitions +
                    " for topic " + topic + " with consumers: " + curConsumers);

            for (ConsumerThreadId consumerThreadId : consumerThreadIdSet) {
                int myConsumerPosition = curConsumers.indexOf(consumerThreadId);
                assert (myConsumerPosition >= 0);
                int startPart = nPartsPerConsumer * Math.min(myConsumerPosition + myConsumerPosition, nConsumersWithExtraPart);
                int nParts = nPartsPerConsumer + ((myConsumerPosition + 1 > nConsumersWithExtraPart) ? 0 : 1);

                /**
                 *   Range-partition the sorted partitions to consumers for better locality.
                 *  The first few consumers pick up an extra partition, if any.
                 */
                if (nParts <= 0)
                    warn("No broker partitions consumed by consumer thread " + consumerThreadId + " for topic " + topic);
                else {
                    for (int i = startPart; i < startPart + nParts; i++) {
                        Integer partition = curPartitions.get(i);
                        info(consumerThreadId + " attempting to claim partition " + partition);
                        // record the partition ownership decision;
                        partitionOwnershipDecision.put(new TopicAndPartition(topic, partition), consumerThreadId);
                    }
                }
            }
        });

        return partitionOwnershipDecision;
    }
}
