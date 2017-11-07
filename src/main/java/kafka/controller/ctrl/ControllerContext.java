package kafka.controller.ctrl;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import kafka.cluster.Broker;
import kafka.controller.ControllerChannelManager;
import kafka.controller.ReassignedPartitionsContext;
import kafka.log.TopicAndPartition;
import kafka.server.KafkaController;
import kafka.utils.Sc;
import org.I0Itec.zkclient.ZkClient;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author zhoulf
 * @create 2017-11-02 16:02
 **/

public class ControllerContext {
    public ZkClient zkClient;
    public Integer zkSessionTimeout;

    public ControllerContext(ZkClient zkClient, java.lang.Integer zkSessionTimeout) {
        this.zkClient = zkClient;
        this.zkSessionTimeout = zkSessionTimeout;
    }

    public ControllerChannelManager controllerChannelManager = null;
    public ReentrantLock controllerLock = new ReentrantLock();
    public Set<Integer> shuttingDownBrokerIds = Sets.newHashSet();
    public Object brokerShutdownLock = new Object();
    public Integer epoch = KafkaController.InitialControllerEpoch - 1;
    public Integer epochZkVersion = KafkaController.InitialControllerEpochZkVersion - 1;
    public AtomicInteger correlationId = new AtomicInteger(0);
    public Set<String> allTopics = Sets.newHashSet();
    public Map<TopicAndPartition, List<Integer>> partitionReplicaAssignment = Maps.newHashMap();
    public Map<TopicAndPartition, LeaderIsrAndControllerEpoch> partitionLeadershipInfo = Maps.newHashMap();
    public Map<TopicAndPartition, ReassignedPartitionsContext> partitionsBeingReassigned = Maps.newHashMap();
    public Set<TopicAndPartition> partitionsUndergoingPreferredReplicaElection = Sets.newHashSet();

    private Set<Broker> liveBrokersUnderlying = Sets.newHashSet();
    private Set<Integer> liveBrokerIdsUnderlying = Sets.newHashSet();

    // setter;
    public void liveBrokers_(Set<Broker> brokers) {
        liveBrokersUnderlying = brokers;
        liveBrokerIdsUnderlying = Sc.map(liveBrokersUnderlying, b -> b.id);
    }

    // getter;
    public Set<Broker> liveBrokers() {
        return Sc.filter(liveBrokersUnderlying, broker -> !shuttingDownBrokerIds.contains(broker.id));
    }

    public Set<Integer> liveBrokerIds() {
        return Sc.filter(liveBrokerIdsUnderlying, brokerId -> !shuttingDownBrokerIds.contains(brokerId));
    }

    public Set<Integer> liveOrShuttingDownBrokerIds() {
        return liveBrokerIdsUnderlying;
    }

    public Set<Broker> liveOrShuttingDownBrokers() {
        return liveBrokersUnderlying;
    }

    public Set<TopicAndPartition> partitionsOnBroker(Integer brokerId) {
        return Sc.toSet(
                Sc.map(Sc.filter(partitionReplicaAssignment, (topicAndPartition, replicas) -> replicas.contains(brokerId))
                        , (topicAndPartition, replicas) -> topicAndPartition)
        );
    }

    public Set<PartitionAndReplica> replicasOnBrokers(Set<Integer> brokerIds) {
        Set<PartitionAndReplica> result = Sets.newHashSet();
        brokerIds.forEach(brokerId ->
                Sc.filter(partitionReplicaAssignment, (topicAndPartition, replicas) -> replicas.contains(brokerId))
                        .forEach((topicAndPartition, replicas) -> result.add(new PartitionAndReplica(topicAndPartition.topic, topicAndPartition.partition, brokerId))
                        ));
        return result;
    }

    public Set<PartitionAndReplica> replicasForTopic(String topic) {
        Set<PartitionAndReplica> result = Sets.newHashSet();
        Sc.filter(partitionReplicaAssignment, (topicAndPartition, replicas) -> topicAndPartition.topic.equals(topic))
                .forEach((topicAndPartition, replicas) ->
                        replicas.forEach(r -> new PartitionAndReplica(topicAndPartition.topic, topicAndPartition.partition, r)));
        return result;
    }

    public Set<TopicAndPartition> partitionsForTopic(String topic) {
        return Sc.filter(partitionReplicaAssignment, (topicAndPartition, replicas) -> topicAndPartition.topic.equals(topic)).keySet();
    }

    public Set<PartitionAndReplica> allLiveReplicas() {
        return replicasOnBrokers(liveBrokerIds());
    }

    public Set<PartitionAndReplica> replicasForPartition(Set<TopicAndPartition> partitions) {
        Set<PartitionAndReplica> result = Sets.newHashSet();
        partitions.forEach(p -> {
            List<Integer> replicas = partitionReplicaAssignment.get(p);
            replicas.forEach(r -> new PartitionAndReplica(p.topic, p.partition, r));
        });
        return result;
    }

    public void removeTopic(String topic) {
        partitionLeadershipInfo = Sc.filter(partitionLeadershipInfo, (topicAndPartition, info) -> topicAndPartition.topic != topic);
        partitionReplicaAssignment = Sc.filter(partitionReplicaAssignment, (topicAndPartition, list) -> topicAndPartition.topic != topic);
        allTopics.remove(topic);
    }
}
