package kafka.cluster;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.yammer.metrics.core.Gauge;
import kafka.admin.AdminUtils;
import kafka.api.LeaderAndIsr;
import kafka.api.PartitionStateInfo;
import kafka.common.ErrorMapping;
import kafka.controller.KafkaController;
import kafka.controller.ctrl.LeaderIsrAndControllerEpoch;
import kafka.func.Tuple;
import kafka.log.*;
import kafka.message.ByteBufferMessageSet;
import kafka.metrics.KafkaMetricsGroup;
import kafka.server.LogOffsetMetadata;
import kafka.server.OffsetManager;
import kafka.server.ReplicaManager;
import kafka.utils.*;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.apache.kafka.common.errors.NotLeaderForPartitionException;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Data structure that represents a topic partition. The leader maintains the AR, ISR, CUR, RAR
 */
public class Partition extends KafkaMetricsGroup {
    public String topic;
    public Integer partitionId;
    public Time time;
    public ReplicaManager replicaManager;

    public Partition(String topic, Integer partitionId, Time time, ReplicaManager replicaManager) {
        this.topic = topic;
        this.partitionId = partitionId;
        this.time = time;
        this.replicaManager = replicaManager;
        localBrokerId = replicaManager.config.brokerId;
        logManager = replicaManager.logManager;
        zkClient = replicaManager.zkClient;
        this.logIdent = String.format("Partition <%s,%d> on broker %d: ", topic, partitionId, localBrokerId);

        newGauge("UnderReplicated", new Gauge<Integer>() {
                    public Integer value() {
                        if (isUnderReplicated()) return 1;
                        else return 0;
                    }
                },
                ImmutableMap.of("topic", topic, "partition", partitionId.toString())
        );
    }

    private Integer localBrokerId;
    private LogManager logManager;
    private ZkClient zkClient;
    private Pool<Integer, Replica> assignedReplicaMap = new Pool<>();
    //        // The read lock is only required when multiple reads are executed and needs to be in a consistent manner;
    private ReentrantReadWriteLock leaderIsrUpdateLock = new ReentrantReadWriteLock();
    private Integer zkVersion = LeaderAndIsr.initialZKVersion;
    private volatile Integer leaderEpoch = LeaderAndIsr.initialLeaderEpoch - 1;
    public volatile Optional<Integer> leaderReplicaIdOpt = Optional.empty();
    public volatile Set<Replica> inSyncReplicas = Sets.newHashSet();

    /* Epoch of the controller that last changed the leader. This needs to be initialized correctly upon broker startup.
 * One way of doing that is through the controller's start replica state change command. When a new broker starts up
 * the controller sends it a start replica command containing the leader for each partition that the broker hosts.
 * In addition to the leader, the controller can also send the epoch of the controller that elected the leader for
 * each partition. */
    private Integer controllerEpoch = KafkaController.InitialControllerEpoch - 1;


    private Boolean isReplicaLocal(Integer replicaId) {
        return (replicaId == localBrokerId);
    }


    public Boolean isUnderReplicated() {
        Optional<Replica> opt = leaderReplicaIfLocal();
        if (opt.isPresent()) {
            return inSyncReplicas.size() < assignedReplicas().size();
        }
        return false;
    }

    public Replica getOrCreateReplica() {
        return getOrCreateReplica(localBrokerId);
    }


    public Replica getOrCreateReplica(Integer replicaId) {
        Optional<Replica> replicaOpt = getReplica(replicaId);
        if (replicaOpt.isPresent()) {
            return replicaOpt.get();
        } else {
            if (isReplicaLocal(replicaId)) {
                LogConfig config = LogConfig.fromProps(logManager.defaultConfig.toProps(), AdminUtils.fetchTopicConfig(zkClient, topic));
                Log log = logManager.createLog(new TopicAndPartition(topic, partitionId), config);
                OffsetCheckpoint checkpoint = replicaManager.highWatermarkCheckpoints.get(log.dir.getParentFile().getAbsolutePath());
                Map<TopicAndPartition, Long> offsetMap = checkpoint.read();
                if (!offsetMap.containsKey(new TopicAndPartition(topic, partitionId)))
                    warn(String.format("No checkpointed highwatermark is found for partition <%s,%d>", topic, partitionId));
                Long offset = Math.min(offsetMap.getOrDefault(new TopicAndPartition(topic, partitionId), 0L), log.logEndOffset());
                Replica localReplica = new Replica(replicaId, this, time, offset, Optional.of(log));
                addReplicaIfNotExists(localReplica);
            } else {
                Replica remoteReplica = new Replica(replicaId, this, time);
                addReplicaIfNotExists(remoteReplica);
            }
            return getReplica(replicaId).get();
        }
    }

    public Optional<Replica> getReplica() {
        return getReplica(this.localBrokerId);
    }

    public Optional<Replica> getReplica(Integer replicaId) {
        if (replicaId == null) {
            replicaId = localBrokerId;
        }
        Replica replica = assignedReplicaMap.get(replicaId);
        if (replica == null)
            return Optional.empty();
        else
            return Optional.of(replica);
    }

    public Optional<Replica> leaderReplicaIfLocal() {
        if (leaderReplicaIdOpt.isPresent()) {
            Integer leaderReplicaId = leaderReplicaIdOpt.get();
            if (leaderReplicaId == localBrokerId) {
                return getReplica(localBrokerId);
            }
        }
        return Optional.empty();
    }


    public void addReplicaIfNotExists(Replica replica) {
        assignedReplicaMap.putIfNotExists(replica.brokerId, replica);
    }

    public Set<Replica> assignedReplicas() {
        return Sc.toSet(assignedReplicaMap.values());
    }

    public void removeReplica(Integer replicaId) {
        assignedReplicaMap.remove(replicaId);
    }

    public void delete() {
        // need to hold the lock to prevent appendMessagesToLeader() from hitting I/O exceptions due to log being deleted;
        Utils.inWriteLock(leaderIsrUpdateLock, () -> {
            assignedReplicaMap.clear();
            Set<Replica> inSyncReplicas = Sets.newHashSet();
            leaderReplicaIdOpt = Optional.empty();
            logManager.deleteLog(new TopicAndPartition(topic, partitionId));
//                error(String.format("Error deleting the log for partition <%s,%d>", topic, partitionId), e);
//                Runtime.getRuntime().halt(1);
            return null;
        });
    }


    public Integer getLeaderEpoch() {
        return this.leaderEpoch;
    }

    /**
     * Make the local replica the leader by resetting LogEndOffset for remote replicas (there could be old LogEndOffset from the time when this broker was the leader last time)
     * and setting the new leader and ISR
     */
    public Boolean makeLeader(Integer controllerId,
                              PartitionStateInfo partitionStateInfo, Integer correlationId,
                              kafka.server.OffsetManager offsetManager) {
        return Utils.inWriteLock(leaderIsrUpdateLock, () -> {
            Set<Integer> allReplicas = partitionStateInfo.allReplicas;
            LeaderIsrAndControllerEpoch leaderIsrAndControllerEpoch = partitionStateInfo.leaderIsrAndControllerEpoch;
            LeaderAndIsr leaderAndIsr = leaderIsrAndControllerEpoch.leaderAndIsr;
            // record the epoch of the controller that made the leadership decision. This is useful while updating the isr;
            // to maintain the decision maker controller's epoch in the zookeeper path;
            Integer controllerEpoch = leaderIsrAndControllerEpoch.controllerEpoch;
            // add replicas that are new;
            allReplicas.forEach(replica -> getOrCreateReplica(replica));
            Set<Replica> newInSyncReplicas = Sc.toSet(Sc.map(leaderAndIsr.isr, r -> getOrCreateReplica(r)));
            // remove assigned replicas that have been removed by the controller;
//            (assignedReplicas().stream().map(r->r.brokerId)-- allReplicas).foreach(removeReplica(_));
            Set<Integer> replicaBrokerIdList = Sc.map(assignedReplicas(), r -> r.brokerId);
            allReplicas.stream().filter(r -> !replicaBrokerIdList.contains(r)).forEach(this::removeReplica);
            inSyncReplicas = newInSyncReplicas;
            leaderEpoch = leaderAndIsr.leaderEpoch;
            zkVersion = leaderAndIsr.zkVersion;
            leaderReplicaIdOpt = Optional.of(localBrokerId);
            // construct the high watermark metadata for the new leader replica;
            Replica newLeaderReplica = getReplica().get();
            newLeaderReplica.convertHWToLocalOffsetMetadata();
            // reset log end offset for remote replicas;
            assignedReplicas().forEach(r -> {
                if (r.brokerId != localBrokerId) {
                    r.logEndOffset_(LogOffsetMetadata.UnknownOffsetMetadata);
                }
            });
            // we may need to increment high watermark since ISR could be down to 1;
            maybeIncrementLeaderHW(newLeaderReplica);
            if (topic == OffsetManager.OffsetsTopicName)
                offsetManager.loadOffsetsFromLog(partitionId);
            return true;
        });
    }

    /**
     * Make the local replica the follower by setting the new leader and ISR to empty
     * If the leader replica id does not change, return false to indicate the replica manager
     */
    public Boolean makeFollower(
            Integer controllerId,
            PartitionStateInfo partitionStateInfo,
            Integer correlationId, OffsetManager offsetManager)

    {
        return Utils.inWriteLock(leaderIsrUpdateLock, () -> {
            Set<Integer> allReplicas = partitionStateInfo.allReplicas;
            LeaderIsrAndControllerEpoch leaderIsrAndControllerEpoch = partitionStateInfo.leaderIsrAndControllerEpoch;
            LeaderAndIsr leaderAndIsr = leaderIsrAndControllerEpoch.leaderAndIsr;
            Integer newLeaderBrokerId = leaderAndIsr.leader;
            // record the epoch of the controller that made the leadership decision. This is useful while updating the isr;
            // to maintain the decision maker controller's epoch in the zookeeper path;
            controllerEpoch = leaderIsrAndControllerEpoch.controllerEpoch;
            // add replicas that are new;
            allReplicas.forEach(r -> getOrCreateReplica(r));
            // remove assigned replicas that have been removed by the controller;
//        (assignedReplicas().map(_.brokerId)-- allReplicas).foreach(removeReplica(_));
            Set<Integer> replicaBrokerIdList = Sc.map(assignedReplicas(), r -> r.brokerId);
            allReplicas.stream().filter(r -> !replicaBrokerIdList.contains(r)).forEach(this::removeReplica);
            inSyncReplicas = Sets.newHashSet();
            leaderEpoch = leaderAndIsr.leaderEpoch;
            zkVersion = leaderAndIsr.zkVersion;

            if (leaderReplicaIdOpt.isPresent()) {
                Integer leaderReplica = leaderReplicaIdOpt.get();
                if (topic == OffsetManager.OffsetsTopicName && leaderReplica == localBrokerId)
                    /* if we are making a leader->follower transition */ ;
                offsetManager.clearOffsetsInPartition(partitionId);
            }

            if (leaderReplicaIdOpt.isPresent() && leaderReplicaIdOpt.get() == newLeaderBrokerId) {
                return false;
            } else {
                leaderReplicaIdOpt = Optional.of(newLeaderBrokerId);
                return true;
            }
        });
    }

    public void updateLeaderHWAndMaybeExpandIsr(Integer replicaId) {
        Utils.inWriteLock(leaderIsrUpdateLock, () -> {
            // check if this replica needs to be added to the ISR;
            Optional<Replica> opt = leaderReplicaIfLocal();
            if (opt.isPresent()) {
                Replica leaderReplica = opt.get();
                Replica replica = getReplica(replicaId).get();
                LogOffsetMetadata leaderHW = leaderReplica.highWatermark();
                // For a replica to get added back to ISR, it has to satisfy 3 conditions-;
                // 1. It is not already in the ISR;
                // 2. It is part of the assigned replica list. See KAFKA-1097;
                // 3. It's log end offset >= leader's high watermark;
                if (!inSyncReplicas.contains(replica) &&
                        Sc.map(assignedReplicas(), r -> r.brokerId).contains(replicaId) &&
                        replica.logEndOffset().offsetDiff(leaderHW) >= 0) {
                    // expand ISR;
                    Set<Replica> newInSyncReplicas = Sets.newHashSet(inSyncReplicas);
                    newInSyncReplicas.add(replica);
                    info(String.format("Expanding ISR for partition <%s,%d> from %s to %s",
                            topic, partitionId, Sc.map(inSyncReplicas, r -> r.brokerId),
                            Sc.map(newInSyncReplicas, r -> r.brokerId)));
                    // update ISR in ZK and cache;
                    updateIsr(newInSyncReplicas);
                    replicaManager.isrExpandRate.mark();
                }
                maybeIncrementLeaderHW(leaderReplica);
            } else {// nothing to do if no longer leader;
            }
            return null;
        });
    }

    public Tuple<Boolean, Short> checkEnoughReplicasReachOffset(Long requiredOffset, Integer requiredAcks) {
        Optional<Replica> opt = leaderReplicaIfLocal();
        if (opt.isPresent()) {
            Replica leaderReplica = opt.get();
            Set<Replica> curInSyncReplicas = inSyncReplicas;
            long numAcks = curInSyncReplicas.stream().filter(r -> {
                if (!r.isLocal())
                    return r.logEndOffset().messageOffset >= requiredOffset;
                else
                    return true; /* also count the local (leader) replica */
            }).count();
            Integer minIsr = leaderReplica.log.get().config.minInSyncReplicas;

            trace(String.format("%d/%d acks satisfied for %s-%d", numAcks, requiredAcks, topic, partitionId));
            if (requiredAcks < 0 && leaderReplica.highWatermark().messageOffset >= requiredOffset) {
          /*
          * requiredAcks < 0 means acknowledge after all replicas in ISR
          * are fully caught up to the (local) leader's offset
          * corresponding to this produce request.
          *
          * minIsr means that the topic is configured not to accept messages
          * if there are not enough replicas in ISR
          * in this scenario the request was already appended locally and
          * then added to the purgatory before the ISR was shrunk
          */
                if (minIsr <= curInSyncReplicas.size()) {
                    return Tuple.of(true, ErrorMapping.NoError);
                } else {
                    return Tuple.of(true, ErrorMapping.NotEnoughReplicasAfterAppendCode);
                }
            } else if (requiredAcks > 0 && numAcks >= requiredAcks) {
                return Tuple.of(true, ErrorMapping.NoError);
            } else {
                return Tuple.of(false, ErrorMapping.NoError);
            }
        } else {
            return Tuple.of(false, ErrorMapping.NotLeaderForPartitionCode);
        }
    }


    /**
     * There is no need to acquire the leaderIsrUpdate lock here since all callers of this private API acquire that lock
     *
     * @param leaderReplica
     */
    private void maybeIncrementLeaderHW(Replica leaderReplica) {
        Set<LogOffsetMetadata> allLogEndOffsets = Sc.map(inSyncReplicas, r -> r.logEndOffset());
        LogOffsetMetadata newHighWatermark = allLogEndOffsets.stream().min(LogOffsetMetadata::compare).get();
        LogOffsetMetadata oldHighWatermark = leaderReplica.highWatermark();
        if (oldHighWatermark.precedes(newHighWatermark)) {
            leaderReplica.highWatermark_(newHighWatermark);
            debug(String.format("High watermark for partition <%s,%d> updated to %s", topic, partitionId, newHighWatermark));
            // some delayed requests may be unblocked after HW changed;
            TopicAndPartition requestKey = new TopicAndPartition(this.topic, this.partitionId);
            replicaManager.unblockDelayedFetchRequests(requestKey);
            replicaManager.unblockDelayedProduceRequests(requestKey);
        } else {
            debug(String.format("Skipping update high watermark since Old hw %s is larger than new hw %s for partition <%s,%d>. All leo's are %s",
                    oldHighWatermark, newHighWatermark, topic, partitionId, allLogEndOffsets.toString()));
        }
    }


    public void maybeShrinkIsr(Long replicaMaxLagTimeMs, Long replicaMaxLagMessages) {
        Utils.inWriteLock(leaderIsrUpdateLock, () -> {
            Optional<Replica> opt = leaderReplicaIfLocal();
            if (opt.isPresent()) {
                Replica leaderReplica = opt.get();
                Set<Replica> outOfSyncReplicas = getOutOfSyncReplicas(leaderReplica, replicaMaxLagTimeMs, replicaMaxLagMessages);
                if (outOfSyncReplicas.size() > 0) {
                    Set<Replica> newInSyncReplicas = Sets.newHashSet(inSyncReplicas);
                    newInSyncReplicas.remove(outOfSyncReplicas);
                    assert (newInSyncReplicas.size() > 0);
                    info(String.format("Shrinking ISR for partition <%s,%d> from %s to %s", topic, partitionId,
                            Sc.map(inSyncReplicas, r -> r.brokerId), Sc.map(newInSyncReplicas, r -> r.brokerId)));
                    // update ISR in zk and in cache;
                    updateIsr(newInSyncReplicas);
                    // we may need to increment high watermark since ISR could be down to 1;
                    maybeIncrementLeaderHW(leaderReplica);
                    replicaManager.isrShrinkRate.mark();
                }
            } else {
                // do nothing if no longer leader;
            }
            return null;
        });
    }

    public Set<Replica> getOutOfSyncReplicas(Replica leaderReplica, Long keepInSyncTimeMs, Long keepInSyncMessages) {
        /**
         * there are two cases that need to be handled here -
         * 1. Stuck If followers the leo of the replica hasn't been updated for keepInSyncTimeMs ms,
         *                     the follower is stuck and should be removed from the ISR
         * 2. Slow If followers the leo of the slowest follower is behind the leo of the leader by keepInSyncMessages, the
         *                     follower is not catching up and should be removed from the ISR
         **/
        LogOffsetMetadata leaderLogEndOffset = leaderReplica.logEndOffset();
        Set<Replica> candidateReplicas = Sets.newHashSet(inSyncReplicas);
        candidateReplicas.remove(leaderReplica);
        // Case 1 above;
        Set<Replica> stuckReplicas = Utils.filter(candidateReplicas, r -> (time.milliseconds() - r.logEndOffsetUpdateTimeMs()) > keepInSyncTimeMs);
        if (stuckReplicas.size() > 0)
            debug(String.format("Stuck replicas for partition <%s,%d> are %s", topic, partitionId, Sc.map(stuckReplicas, r -> r.brokerId)));
        // Case 2 above;
        Set<Replica> slowReplicas = Utils.filter(candidateReplicas, r -> r.logEndOffset().messageOffset >= 0 &&
                leaderLogEndOffset.messageOffset - r.logEndOffset().messageOffset > keepInSyncMessages);

        if (slowReplicas.size() > 0)
            debug(String.format("Slow replicas for partition <%s,%d> are %s", topic, partitionId, Sc.map(slowReplicas, r -> r.brokerId)));
        stuckReplicas.addAll(slowReplicas);
        return stuckReplicas;

    }

    public void appendMessagesToLeader(ByteBufferMessageSet messages) {
        appendMessagesToLeader(messages, 0);
    }

    public LogAppendInfo appendMessagesToLeader(ByteBufferMessageSet messages, Integer requiredAcks) {
        return Utils.inReadLock(leaderIsrUpdateLock, () -> {
            Optional<Replica> leaderReplicaOpt = leaderReplicaIfLocal();
            if (leaderReplicaOpt.isPresent()) {
                Replica leaderReplica = leaderReplicaOpt.get();
                Log log = leaderReplica.log.get();
                Integer minIsr = log.config.minInSyncReplicas;
                Integer inSyncSize = inSyncReplicas.size();

                // Avoid writing to leader if there are not enough insync replicas to make it safe;
                if (inSyncSize < minIsr && requiredAcks == -1) {
                    throw new NotEnoughReplicasException(String.format("Number of insync replicas for partition <%s,%d> is <%d>, below required minimum <%d>"
                            , topic, partitionId, minIsr, inSyncSize));
                }

                LogAppendInfo info = log.append(messages, true);
                // probably unblock some follower fetch requests since log end offset has been updated;
                replicaManager.unblockDelayedFetchRequests(new TopicAndPartition(this.topic, this.partitionId));
                // we may need to increment high watermark since ISR could be down to 1;
                maybeIncrementLeaderHW(leaderReplica);
                return info;
            } else {
                throw new NotLeaderForPartitionException(String.format("Leader not local for partition <%s,%d> on broker %d",
                        topic, partitionId, localBrokerId));
            }
        });
    }

    private void updateIsr(Set<Replica> newIsr) {
        LeaderAndIsr newLeaderAndIsr = new LeaderAndIsr(localBrokerId, leaderEpoch, Sc.toList(Sc.map(newIsr, r -> r.brokerId)), zkVersion);
        Tuple<Boolean, Integer> result = ReplicationUtils.updateLeaderAndIsr(zkClient, topic, partitionId,
                newLeaderAndIsr, controllerEpoch, zkVersion);
        Boolean updateSucceeded = result.v1;
        Integer newVersion = result.v2;
        if (updateSucceeded) {
            inSyncReplicas = newIsr;
            zkVersion = newVersion;
            trace(String.format("ISR updated to <%s> and zkVersion updated to <%d>", newIsr, zkVersion));
        } else {
            info(String.format("Cached zkVersion <%d> not equal to that in zookeeper, skip updating ISR", zkVersion));
        }
    }

    @Override
    public boolean equals(Object that) {
        if (!(that instanceof Partition))
            return false;
        Partition other = (Partition) that;
        if (topic.equals(other.topic) && partitionId == other.partitionId)
            return true;
        return false;
    }

    @Override
    public int hashCode() {
        return 31 + topic.hashCode() + 17 * partitionId;
    }

    @Override
    public String toString() {
        StringBuilder partitionString = new StringBuilder();
        partitionString.append("Topic: " + topic);
        partitionString.append("; Partition: " + partitionId);
        partitionString.append("; Leader: " + leaderReplicaIdOpt);
        partitionString.append("; AssignedReplicas: " + assignedReplicaMap.keys());
        partitionString.append("; InSyncReplicas: " + Sc.map(inSyncReplicas, r -> r.brokerId));
        return partitionString.toString();
    }

}
