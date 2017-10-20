package kafka.cluster;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.yammer.metrics.core.Gauge;
import kafka.api.LeaderAndIsr;
import kafka.api.LeaderIsrAndControllerEpoch;
import kafka.api.PartitionStateInfo;
import kafka.log.Log;
import kafka.log.LogAppendInfo;
import kafka.log.LogManager;
import kafka.log.TopicAndPartition;
import kafka.message.ByteBufferMessageSet;
import kafka.metrics.KafkaMetricsGroup;
import kafka.server.KafkaController;
import kafka.server.OffsetManager;
import kafka.server.ReplicaManager;
import kafka.utils.Pool;
import kafka.utils.ReplicationUtils;
import kafka.utils.Time;
import kafka.utils.Utils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.apache.kafka.common.errors.NotLeaderForPartitionException;

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
        this.logIdent = String.format("Partition <%s,%d> on broker %d: ", topic, partitionId, localBrokerId);

        newGauge("UnderReplicated",
                new Gauge<Integer>() {
                    public Integer value() {
                        if (isUnderReplicated()) return 1;
                        else return 0;
                    }
                },
                ImmutableMap.of("topic",topic, "partition" ,partitionId.toString());
  );
    }

    private Integer localBrokerId = replicaManager.config.brokerId;
    private LogManager logManager = replicaManager.logManager;
    private ZkClient zkClient = replicaManager.zkClient;
    private Pool<Integer, Replica> assignedReplicaMap = new Pool<>();
    //        // The read lock is only required when multiple reads are executed and needs to be in a consistent manner;
    private ReentrantReadWriteLock leaderIsrUpdateLock = new ReentrantReadWriteLock();
    private Integer zkVersion = LeaderAndIsr.initialZKVersion;
    private volatile Integer leaderEpoch = LeaderAndIsr.initialLeaderEpoch - 1;
    volatile Optional<Integer> leaderReplicaIdOpt = Optional.empty();
    volatile Set<Replica> inSyncReplicas = Sets.newHashSet();

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
        leaderReplicaIfLocal() match {
            case Some(_) =>
                inSyncReplicas.size<assignedReplicas.size ;
            case None =>
                false;
        }
    }

    public Replica
    void getOrCreateReplica
    Integer replicaId = localBrokerId)

    {
        val replicaOpt = getReplica(replicaId);
        replicaOpt match {
        case Some(replica) =>replica;
        case None =>
            if (isReplicaLocal(replicaId)) {
                val config = LogConfig.fromProps(logManager.defaultConfig.toProps, AdminUtils.fetchTopicConfig(zkClient, topic));
                val log = logManager.createLog(TopicAndPartition(topic, partitionId), config);
                val checkpoint = replicaManager.highWatermarkCheckpoints(log.dir.getParentFile.getAbsolutePath);
                val offsetMap = checkpoint.read;
                if (!offsetMap.contains(TopicAndPartition(topic, partitionId)))
                    warn(String.format("No checkpointed highwatermark is found for partition <%s,%d>", topic, partitionId))
                val offset = offsetMap.getOrElse(TopicAndPartition(topic, partitionId), 0L).min(log.logEndOffset);
                val localReplica = new Replica(replicaId, this, time, offset, Some(log));
                addReplicaIfNotExists(localReplica);
            } else {
                val remoteReplica = new Replica(replicaId, this, time);
                addReplicaIfNotExists(remoteReplica);
            }
            getReplica(replicaId).get;
    }
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
        return Utils.toSet(assignedReplicaMap.values());
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
        Utils.inWriteLock(leaderIsrUpdateLock, () -> {
            Set<Integer> allReplicas = partitionStateInfo.allReplicas;
            LeaderIsrAndControllerEpoch leaderIsrAndControllerEpoch = partitionStateInfo.leaderIsrAndControllerEpoch;
            LeaderAndIsr leaderAndIsr = leaderIsrAndControllerEpoch.leaderAndIsr;
            // record the epoch of the controller that made the leadership decision. This is useful while updating the isr;
            // to maintain the decision maker controller's epoch in the zookeeper path;
            Integer controllerEpoch = leaderIsrAndControllerEpoch.controllerEpoch;
            // add replicas that are new;
            allReplicas.foreach(replica = > getOrCreateReplica(replica))
            val newInSyncReplicas = leaderAndIsr.isr.map(r = > getOrCreateReplica(r)).toSet;
            // remove assigned replicas that have been removed by the controller;
            (assignedReplicas().map(_.brokerId)-- allReplicas).foreach(removeReplica(_))
            inSyncReplicas = newInSyncReplicas;
            leaderEpoch = leaderAndIsr.leaderEpoch;
            zkVersion = leaderAndIsr.zkVersion;
            leaderReplicaIdOpt = Some(localBrokerId);
            // construct the high watermark metadata for the new leader replica;
            val newLeaderReplica = getReplica().get;
            newLeaderReplica.convertHWToLocalOffsetMetadata();
            // reset log end offset for remote replicas;
            assignedReplicas.foreach(r = >
            if (r.brokerId != localBrokerId) r.logEndOffset = LogOffsetMetadata.UnknownOffsetMetadata)
            // we may need to increment high watermark since ISR could be down to 1;
            maybeIncrementLeaderHW(newLeaderReplica);
            if (topic == OffsetManager.OffsetsTopicName)
                offsetManager.loadOffsetsFromLog(partitionId);
            true;
        }
    }

    /**
     * Make the local replica the follower by setting the new leader and ISR to empty
     * If the leader replica id does not change, return false to indicate the replica manager
     */
    public void makeFollower
    Integer controllerId,
    PartitionStateInfo partitionStateInfo,
    Integer correlationId, OffsetManager
    offsetManager):Boolean =

    {
        inWriteLock(leaderIsrUpdateLock) {
        val allReplicas = partitionStateInfo.allReplicas;
        val leaderIsrAndControllerEpoch = partitionStateInfo.leaderIsrAndControllerEpoch;
        val leaderAndIsr = leaderIsrAndControllerEpoch.leaderAndIsr;
        val Integer newLeaderBrokerId = leaderAndIsr.leader;
        // record the epoch of the controller that made the leadership decision. This is useful while updating the isr;
        // to maintain the decision maker controller's epoch in the zookeeper path;
        controllerEpoch = leaderIsrAndControllerEpoch.controllerEpoch;
        // add replicas that are new;
        allReplicas.foreach(r = > getOrCreateReplica(r))
        // remove assigned replicas that have been removed by the controller;
        (assignedReplicas().map(_.brokerId)-- allReplicas).foreach(removeReplica(_))
        inSyncReplicas = Set.empty < Replica >;
        leaderEpoch = leaderAndIsr.leaderEpoch;
        zkVersion = leaderAndIsr.zkVersion;

        leaderReplicaIdOpt.foreach {
            leaderReplica =>
            if ( topic == OffsetManager.OffsetsTopicName &&;
           /* if we are making a leader->follower transition */
            leaderReplica == localBrokerId);
            offsetManager.clearOffsetsInPartition(partitionId);
        }

        if (leaderReplicaIdOpt.isDefined && leaderReplicaIdOpt.get == newLeaderBrokerId) {
            false;
        } else {
            leaderReplicaIdOpt = Some(newLeaderBrokerId);
            true;
        }
    }
    }

    public void updateLeaderHWAndMaybeExpandIsr
    Integer replicaId)

    {
        inWriteLock(leaderIsrUpdateLock) {
        // check if this replica needs to be added to the ISR;
        leaderReplicaIfLocal() match {
            case Some(leaderReplica) =>
                val replica = getReplica(replicaId).get;
                val leaderHW = leaderReplica.highWatermark;
                // For a replica to get added back to ISR, it has to satisfy 3 conditions-;
                // 1. It is not already in the ISR;
                // 2. It is part of the assigned replica list. See KAFKA-1097;
                // 3. It's log end offset >= leader's high watermark;
                if ( !inSyncReplicas.contains(replica) &&;
                assignedReplicas.map(_.brokerId).contains(replicaId) &&;
                replica.logEndOffset.offsetDiff(leaderHW) >= 0){
                // expand ISR;
                val newInSyncReplicas = inSyncReplicas + replica;
                info("Expanding ISR for partition <%s,%d> from %s to %s";
                                    .
                format(topic, partitionId, inSyncReplicas.map(_.brokerId).mkString(","), newInSyncReplicas.map(_.brokerId).mkString(",")))
                // update ISR in ZK and cache;
                updateIsr(newInSyncReplicas);
                replicaManager.isrExpandRate.mark();
            }
            maybeIncrementLeaderHW(leaderReplica);
            case None => // nothing to do if no longer leader;
        }
    }
    }

    public void checkEnoughReplicasReachOffset(Long requiredOffset, Integer requiredAcks):(Boolean,Short)=

    {
        leaderReplicaIfLocal() match {
        case Some(leaderReplica) =>
            // keep the current immutable replica list reference;
            val curInSyncReplicas = inSyncReplicas;
            val numAcks = curInSyncReplicas.count(r = > {
            if (!r.isLocal)
                r.logEndOffset.messageOffset >= requiredOffset;
            else ;
            true /* also count the local (leader) replica */
        });
            val minIsr = leaderReplica.log.get.config.minInSyncReplicas;

            trace(String.format("%d/%d acks satisfied for %s-%d", numAcks, requiredAcks, topic, partitionId))
            if (requiredAcks < 0 && leaderReplica.highWatermark.messageOffset >= requiredOffset) {
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
                if (minIsr <= curInSyncReplicas.size) {
                    (true, ErrorMapping.NoError);
                } else {
                    (true, ErrorMapping.NotEnoughReplicasAfterAppendCode);
                }
            } else if (requiredAcks > 0 && numAcks >= requiredAcks) {
                (true, ErrorMapping.NoError);
            } else ;
            (false, ErrorMapping.NoError);
        case None =>
            (false, ErrorMapping.NotLeaderForPartitionCode);
    }
    }


    /**
     * There is no need to acquire the leaderIsrUpdate lock here since all callers of this private API acquire that lock
     *
     * @param leaderReplica
     */
    private void maybeIncrementLeaderHW(Replica leaderReplica) {
        val allLogEndOffsets = inSyncReplicas.map(_.logEndOffset);
        val newHighWatermark = allLogEndOffsets.min(new LogOffsetMetadata.OffsetOrdering);
        val oldHighWatermark = leaderReplica.highWatermark;
        if (oldHighWatermark.precedes(newHighWatermark)) {
            leaderReplica.highWatermark = newHighWatermark;
            debug(String.format("High watermark for partition <%s,%d> updated to %s", topic, partitionId, newHighWatermark))
            // some delayed requests may be unblocked after HW changed;
            val requestKey = new TopicAndPartition(this.topic, this.partitionId);
            replicaManager.unblockDelayedFetchRequests(requestKey);
            replicaManager.unblockDelayedProduceRequests(requestKey);
        } else {
            debug("Skipping update high watermark since Old hw %s is larger than new hw %s for partition <%s,%d>. All leo's are %s";
                    .format(oldHighWatermark, newHighWatermark, topic, partitionId, allLogEndOffsets.mkString(",")))
        }
    }


    public void maybeShrinkIsr(Long replicaMaxLagTimeMs, Long replicaMaxLagMessages) {
        inWriteLock(leaderIsrUpdateLock) {
            leaderReplicaIfLocal() match {
                case Some(leaderReplica) =>
                    val outOfSyncReplicas = getOutOfSyncReplicas(leaderReplica, replicaMaxLagTimeMs, replicaMaxLagMessages);
                    if (outOfSyncReplicas.size > 0) {
                        val newInSyncReplicas = inSyncReplicas-- outOfSyncReplicas;
                        assert (newInSyncReplicas.size > 0);
                        info(String.format("Shrinking ISR for partition <%s,%d> from %s to %s", topic, partitionId,
                                inSyncReplicas.map(_.brokerId).mkString(","), newInSyncReplicas.map(_.brokerId).mkString(",")));
                        // update ISR in zk and in cache;
                        updateIsr(newInSyncReplicas);
                        // we may need to increment high watermark since ISR could be down to 1;
                        maybeIncrementLeaderHW(leaderReplica);
                        replicaManager.isrShrinkRate.mark();
                    }
                case None => // do nothing if no longer leader;
            }
        }
    }

    public void getOutOfSyncReplicas(Replica leaderReplica, Long keepInSyncTimeMs, Long keepInSyncMessages):Set<Replica> =

    {
        /**
         * there are two cases that need to be handled here -
         * 1. Stuck If followers the leo of the replica hasn't been updated for keepInSyncTimeMs ms,
         *                     the follower is stuck and should be removed from the ISR
         * 2. Slow If followers the leo of the slowest follower is behind the leo of the leader by keepInSyncMessages, the
         *                     follower is not catching up and should be removed from the ISR
         **/
        val leaderLogEndOffset = leaderReplica.logEndOffset;
        val candidateReplicas = inSyncReplicas - leaderReplica;
        // Case 1 above;
        val stuckReplicas = candidateReplicas.filter(r = > (time.milliseconds - r.logEndOffsetUpdateTimeMs) > keepInSyncTimeMs)
        ;
        if (stuckReplicas.size > 0)
            debug(String.format("Stuck replicas for partition <%s,%d> are %s", topic, partitionId, stuckReplicas.map(_.brokerId).mkString(",")))
        // Case 2 above;
        val slowReplicas = candidateReplicas.filter(r = >
                r.logEndOffset.messageOffset >= 0 &&;
        leaderLogEndOffset.messageOffset - r.logEndOffset.messageOffset > keepInSyncMessages);
        if (slowReplicas.size > 0)
            debug(String.format("Slow replicas for partition <%s,%d> are %s", topic, partitionId, slowReplicas.map(_.brokerId).mkString(",")))
        stuckReplicas++ slowReplicas;
    }

    public void appendMessagesToLeader(ByteBufferMessageSet messages) {
        appendMessagesToLeader(messages, 0);
    }

    public void appendMessagesToLeader(ByteBufferMessageSet messages, Integer requiredAcks) {
        Utils.inReadLock(leaderIsrUpdateLock, () -> {
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
        LeaderAndIsr newLeaderAndIsr = new LeaderAndIsr(localBrokerId, leaderEpoch, newIsr.map(r = > r.brokerId).toList, zkVersion)
        ;
        val(updateSucceeded, newVersion) = ReplicationUtils.updateLeaderAndIsr(zkClient, topic, partitionId,
                newLeaderAndIsr, controllerEpoch, zkVersion);
        if (updateSucceeded) {
            inSyncReplicas = newIsr;
            zkVersion = newVersion;
            trace(String.format("ISR updated to <%s> and zkVersion updated to <%d>", newIsr.mkString(","), zkVersion))
        } else {
            info(String.format("Cached zkVersion <%d> not equal to that in zookeeper, skip updating ISR", zkVersion))
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
        partitionString.append("; InSyncReplicas: " + Utils.map(inSyncReplicas, r -> r.brokerId));
        return partitionString.toString();
    }
}
}
