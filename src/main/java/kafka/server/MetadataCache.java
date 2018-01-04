package kafka.server;

/**
 * @author zhoulf
 * @create 2017-10-19 31 15
 **/

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import kafka.api.*;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.ReplicaNotAvailableException;
import kafka.controller.ctrl.LeaderIsrAndControllerEpoch;
import kafka.func.Tuple;
import kafka.log.TopicAndPartition;
import kafka.utils.Logging;
import kafka.utils.Sc;
import kafka.utils.Utils;
import org.apache.kafka.common.errors.LeaderNotAvailableException;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * A cache for the state (e.g., current leader) of each partition. This cache is updated through
 * UpdateMetadataRequest from the controller. Every broker maintains the same cache, asynchronously.
 */
public class MetadataCache extends Logging{
    private Map<String, Map<Integer, PartitionStateInfo>> cache = Maps.newHashMap();
    private Map<Integer, Broker> aliveBrokers = Maps.newHashMap();
    private ReentrantReadWriteLock partitionMetadataLock = new ReentrantReadWriteLock();

    public  List<TopicMetadata> getTopicMetadata(Set<String> topics) {
        boolean isAllTopics = topics.isEmpty();
        Set<String> topicsRequested = isAllTopics ? cache.keySet() : topics;
        List<TopicMetadata> topicResponses = Lists.newArrayList();
        Utils.inReadLock(partitionMetadataLock, () -> {
            for (String topic : topicsRequested) {
                if (isAllTopics || cache.containsKey(topic)) {
                    Map<Integer, PartitionStateInfo> partitionStateInfos = cache.get(topic);
                    List<PartitionMetadata> partitionMetadata= Utils.map(partitionStateInfos, (partitionId, partitionState) -> {
                        Set<Integer> replicas = partitionState.allReplicas;
                        List<Broker> replicaInfo = Sc.map(replicas, r -> aliveBrokers.getOrDefault(r, null)).stream().filter(b -> b != null).collect(Collectors.toList());
                        Broker leaderInfo =null;
                        List<Broker> isrInfo =null;
                        LeaderIsrAndControllerEpoch leaderIsrAndEpoch = partitionState.leaderIsrAndControllerEpoch;
                        Integer leader = leaderIsrAndEpoch.leaderAndIsr.leader;
                        List<Integer> isr = leaderIsrAndEpoch.leaderAndIsr.isr;
                        TopicAndPartition topicPartition = new TopicAndPartition(topic, partitionId);
                        try {
                            leaderInfo = aliveBrokers.get(leader);
                            if (leaderInfo==null)
                                throw new LeaderNotAvailableException(String.format("Leader not available for %s", topicPartition));
                             isrInfo = Sc.map(isr, r -> aliveBrokers.getOrDefault(r, null)).stream().filter(b -> b != null).collect(Collectors.toList());
                            if (replicaInfo.size() < replicas.size())
                                throw new ReplicaNotAvailableException("Replica information not available for following brokers: " +
                                        Sc.filterNot(replicas,r-> Sc.map(replicaInfo, r2->r2.id).contains(r)));
                            if (isrInfo.size() < isr.size()) {
                                List<Broker> isrInfo2 = isrInfo;
                                throw new ReplicaNotAvailableException("In Sync Replica information not available for following brokers: " +
                                        Sc.filterNot(isr, i -> Sc.map(isrInfo2, s -> s.id).contains(i)));
                            }
                            return new PartitionMetadata(partitionId, leaderInfo, replicaInfo, isrInfo, ErrorMapping.NoError);
                        } catch (Throwable e){
                                debug(String.format("Error while fetching metadata for %s. Possible cause: %s", topicPartition, e.getMessage()));
                                return   new PartitionMetadata(partitionId, leaderInfo, replicaInfo, isrInfo, ErrorMapping.codeFor(e.getClass()));
                        }
                    });
                    topicResponses.add(new TopicMetadata(topic, partitionMetadata));
                }
            }
            return null;
        });
        return topicResponses;
    }

    public List<Broker> getAliveBrokers() {
        return Utils.inReadLock(partitionMetadataLock, () ->
                new ArrayList<>(aliveBrokers.values())
        );
    }

    public void addOrUpdatePartitionInfo(String topic,
                                         Integer partitionId,
                                         PartitionStateInfo stateInfo) {
        Utils.inWriteLock(partitionMetadataLock, () -> {
            Map<Integer, PartitionStateInfo> infos = cache.get(topic);
            if (infos != null) {
                infos.put(partitionId, stateInfo);
            }
            Map<Integer, PartitionStateInfo> newInfos = Maps.newHashMap();
            cache.put(topic, newInfos);
            newInfos.put(partitionId, stateInfo);
            return newInfos;
        });
    }

    public Optional<PartitionStateInfo> getPartitionInfo(String topic, Integer partitionId) {
        return Utils.inReadLock(partitionMetadataLock, () -> {
            Map<Integer, PartitionStateInfo> partitionInfos = cache.get(topic);
            if (partitionInfos == null) {
                return Optional.empty();
            }
            return Optional.of(partitionInfos.get(partitionId));
        });
    }

    public void updateCache(UpdateMetadataRequest updateMetadataRequest,
                            Integer brokerId,
                            StateChangeLogger stateChangeLogger) {
        Utils.inWriteLock(partitionMetadataLock, () -> {
            aliveBrokers = Sc.toMap(Sc.map(updateMetadataRequest.aliveBrokers, b -> Tuple.of(b.id, b)));
            Sc.foreach(updateMetadataRequest.partitionStateInfos, (tp, info) -> {
                if (info.leaderIsrAndControllerEpoch.leaderAndIsr.leader == LeaderAndIsr.LeaderDuringDelete) {
                    removePartitionInfo(tp.topic, tp.partition);
                    stateChangeLogger.trace(String.format("Broker %d deleted partition %s from metadata cache in response to UpdateMetadata request " +
                                    "sent by controller %d epoch %d with correlation id %d",
                            brokerId, tp, updateMetadataRequest.controllerId,
                            updateMetadataRequest.controllerEpoch, updateMetadataRequest.correlationId));
                } else {
                    addOrUpdatePartitionInfo(tp.topic, tp.partition, info);
                    stateChangeLogger.trace(String.format("Broker %d cached leader info %s for partition %s in response to UpdateMetadata request " +
                                    "sent by controller %d epoch %d with correlation id %d",
                            brokerId, info, tp, updateMetadataRequest.controllerId,
                            updateMetadataRequest.controllerEpoch, updateMetadataRequest.correlationId));
                }
            });
            return null;
        });
    }

    private boolean removePartitionInfo(String topic, Integer partitionId) {
        Map infos = cache.get(topic);
        if (infos == null) {
            return false;
        }
        infos.remove(partitionId);
        if (infos.isEmpty()) {
            cache.remove(topic);
        }
        return true;
    }
}
