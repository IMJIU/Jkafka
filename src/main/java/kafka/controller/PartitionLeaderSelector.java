package kafka.controller;

import com.google.common.collect.Lists;
import kafka.admin.AdminUtils;
import kafka.api.LeaderAndIsr;
import kafka.common.LeaderElectionNotNeededException;
import kafka.common.NoReplicaOnlineException;
import kafka.common.StateChangeFailedException;
import kafka.controller.ctrl.ControllerContext;
import kafka.controller.ctrl.ControllerStats;
import kafka.func.Tuple;
import kafka.log.LogConfig;
import kafka.log.TopicAndPartition;
import kafka.server.KafkaConfig;
import kafka.utils.Logging;
import kafka.utils.Sc;

import java.util.List;
import java.util.Set;

/**
 * @author zhoulf
 * @create 2017-11-06 52 13
 **/

public interface PartitionLeaderSelector {

    /**
     * @param topicAndPartition   The topic and partition whose leader needs to be elected
     * @param currentLeaderAndIsr The current leader and isr of input partition read from zookeeper
     * @return The leader and isr request, with the newly selected leader and isr, and the set of replicas to receive
     * the LeaderAndIsrRequest.
     * @throws NoReplicaOnlineException If no replica in the assigned replicas list is alive
     */
    Tuple<LeaderAndIsr, List<Integer>> selectLeader(TopicAndPartition topicAndPartition, LeaderAndIsr currentLeaderAndIsr);

}

/**
 * Select the new leader, new isr and receiving replicas (for the LeaderAndIsrRequest):
 * 1. If at least one broker from the isr is alive, it picks a broker from the live isr as the new leader and the live
 * isr as the new isr.
 * 2. Else, if unclean leader election for the topic is disabled, it throws a NoReplicaOnlineException.
 * 3. Else, it picks some alive broker from the assigned replica list as the new leader and the new isr.
 * 4. If no broker in the assigned replica list is alive, it throws a NoReplicaOnlineException
 * Replicas to receive LeaderAndIsr request = live assigned replicas
 * Once the leader is successfully registered in zookeeper, it updates the allLeaders cache
 */
class OfflinePartitionLeaderSelector extends Logging implements PartitionLeaderSelector {
    public ControllerContext controllerContext;
    public KafkaConfig config;

    public OfflinePartitionLeaderSelector(ControllerContext controllerContext, KafkaConfig config) {
        this.controllerContext = controllerContext;
        this.config = config;
        this.logIdent = "<OfflinePartitionLeaderSelector>: ";
    }

    public Tuple<LeaderAndIsr, List<Integer>> selectLeader(TopicAndPartition topicAndPartition, LeaderAndIsr currentLeaderAndIsr) {
        List<Integer> assignedReplicas = controllerContext.partitionReplicaAssignment.get(topicAndPartition);
        if (assignedReplicas != null) {
            List<Integer> liveAssignedReplicas = Sc.filter(assignedReplicas, r -> controllerContext.liveBrokerIds().contains(r));
            List<Integer> liveBrokersInIsr = Sc.filter(currentLeaderAndIsr.isr, r -> controllerContext.liveBrokerIds().contains(r));
            Integer currentLeaderEpoch = currentLeaderAndIsr.leaderEpoch;
            Integer currentLeaderIsrZkPathVersion = currentLeaderAndIsr.zkVersion;
            LeaderAndIsr newLeaderAndIsr;
            if (liveBrokersInIsr.isEmpty()) {
                // Prior to electing an unclean (i.e. non-ISR) leader, ensure that doing so is not disallowed by the configuration;
                // for unclean leader election.;
                if (!LogConfig.fromProps(config.props.props, AdminUtils.fetchTopicConfig(controllerContext.zkClient,
                        topicAndPartition.topic)).uncleanLeaderElectionEnable) {
                    throw new NoReplicaOnlineException(("No broker in ISR for partition " +
                            String.format("%s is alive. Live brokers are: <%s>,", topicAndPartition, controllerContext.liveBrokerIds())) +
                            String.format(" ISR brokers are: <%s>", currentLeaderAndIsr.isr));
                }

                debug(String.format("No broker in ISR is alive for %s. Pick the leader from the alive assigned replicas: %s",
                        topicAndPartition, liveAssignedReplicas));
                if (liveAssignedReplicas.isEmpty()) {
                    throw new NoReplicaOnlineException(("No replica for partition " +
                            String.format("%s is alive. Live brokers are: <%s>,", topicAndPartition, controllerContext.liveBrokerIds())) +
                            String.format(" Assigned replicas are: <%s>", assignedReplicas));
                } else {
                    ControllerStats.uncleanLeaderElectionRate.mark();
                    Integer newLeader = Sc.head(liveAssignedReplicas);
                    warn(String.format("No broker in ISR is alive for %s. Elect leader %d from live brokers %s. There's potential data loss.",
                            topicAndPartition, newLeader, liveAssignedReplicas));
                    newLeaderAndIsr = new LeaderAndIsr(newLeader, currentLeaderEpoch + 1, Lists.newArrayList(newLeader), currentLeaderIsrZkPathVersion + 1);
                }
            } else {
                List<Integer> liveReplicasInIsr = Sc.filter(liveAssignedReplicas, r -> liveBrokersInIsr.contains(r));
                Integer newLeader = Sc.head(liveReplicasInIsr);
                debug(String.format("Some broker in ISR is alive for %s. Select %d from ISR %s to be the leader.",
                        topicAndPartition, newLeader, liveBrokersInIsr));
                newLeaderAndIsr = new LeaderAndIsr(newLeader, currentLeaderEpoch + 1, liveBrokersInIsr, currentLeaderIsrZkPathVersion + 1);
            }
            info(String.format("Selected new leader and ISR %s for offline partition %s", newLeaderAndIsr, topicAndPartition));
            return Tuple.of(newLeaderAndIsr, liveAssignedReplicas);
        } else {
            throw new NoReplicaOnlineException(String.format("Partition %s doesn't have replicas assigned to it", topicAndPartition));
        }
    }
}

/**
 * New leader = a live in-sync reassigned replica
 * New isr = current isr
 * Replicas to receive LeaderAndIsr request = reassigned replicas
 */
class ReassignedPartitionLeaderSelector extends Logging implements PartitionLeaderSelector {
    public ControllerContext controllerContext;

    public ReassignedPartitionLeaderSelector(ControllerContext controllerContext) {
        this.controllerContext = controllerContext;
        this.logIdent = "<ReassignedPartitionLeaderSelector>: ";
    }

    /**
     * The reassigned replicas are already in the ISR when selectLeader is called.
     */
    public Tuple<LeaderAndIsr, List<Integer>> selectLeader(TopicAndPartition topicAndPartition, LeaderAndIsr currentLeaderAndIsr) {
        List<Integer> reassignedInSyncReplicas = controllerContext.partitionsBeingReassigned.get(topicAndPartition).newReplicas;
        Integer currentLeaderEpoch = currentLeaderAndIsr.leaderEpoch;
        Integer currentLeaderIsrZkPathVersion = currentLeaderAndIsr.zkVersion;
        List<Integer> aliveReassignedInSyncReplicas = Sc.filter(reassignedInSyncReplicas, r ->
                controllerContext.liveBrokerIds().contains(r) && currentLeaderAndIsr.isr.contains(r));
        Integer newLeader = Sc.head(aliveReassignedInSyncReplicas);
        if (newLeader != null) {
            return Tuple.of(new LeaderAndIsr(newLeader, currentLeaderEpoch + 1, currentLeaderAndIsr.isr,
                    currentLeaderIsrZkPathVersion + 1), reassignedInSyncReplicas);
        } else {
            if (reassignedInSyncReplicas.size() == 0) {
                throw new NoReplicaOnlineException("List of reassigned replicas for partition " +
                        String.format(" %s is empty. Current leader and ISR: <%s>", topicAndPartition, currentLeaderAndIsr));
            } else {
                throw new NoReplicaOnlineException("None of the reassigned replicas for partition " +
                        String.format("%s are in-sync with the leader. Current leader and ISR: <%s>", topicAndPartition, currentLeaderAndIsr));
            }
        }
    }
}

/**
 * New leader = preferred (first assigned) replica (if in isr and alive);
 * New isr = current isr;
 * Replicas to receive LeaderAndIsr request = assigned replicas
 */
class PreferredReplicaPartitionLeaderSelector extends Logging implements PartitionLeaderSelector {
    public ControllerContext controllerContext;

    public PreferredReplicaPartitionLeaderSelector(ControllerContext controllerContext) {
        this.controllerContext = controllerContext;
        this.logIdent = "<PreferredReplicaPartitionLeaderSelector>: ";
    }

    public Tuple<LeaderAndIsr, List<Integer>> selectLeader(TopicAndPartition topicAndPartition, LeaderAndIsr currentLeaderAndIsr) {
        List<Integer> assignedReplicas = controllerContext.partitionReplicaAssignment.get(topicAndPartition);
        Integer preferredReplica = Sc.head(assignedReplicas);
        // check if preferred replica is the current leader;
        Integer currentLeader = controllerContext.partitionLeadershipInfo.get(topicAndPartition).leaderAndIsr.leader;
        if (currentLeader.equals(preferredReplica)) {
            throw new LeaderElectionNotNeededException(String.format("Preferred replica %d is already the current leader for partition %s",
                    preferredReplica, topicAndPartition));
        } else {
            info(String.format("Current leader %d for partition %s is not the preferred replica.", currentLeader, topicAndPartition) +
                    " Trigerring preferred replica leader election");
            // check if preferred replica is not the current leader and is alive and in the isr;
            if (controllerContext.liveBrokerIds().contains(preferredReplica) && currentLeaderAndIsr.isr.contains(preferredReplica)) {
                return Tuple.of(new LeaderAndIsr(preferredReplica, currentLeaderAndIsr.leaderEpoch + 1, currentLeaderAndIsr.isr,
                        currentLeaderAndIsr.zkVersion + 1), assignedReplicas);
            } else {
                throw new StateChangeFailedException(String.format("Preferred replica %d for partition ", preferredReplica) +
                        String.format("%s is either not alive or not in the isr. Current leader and ISR: <%s>", topicAndPartition, currentLeaderAndIsr));
            }
        }
    }
}

/**
 * New leader = replica in isr that's not being shutdown;
 * New isr = current isr - shutdown replica;
 * Replicas to receive LeaderAndIsr request = live assigned replicas
 */
class ControlledShutdownLeaderSelector extends Logging implements PartitionLeaderSelector {
    public ControllerContext controllerContext;

    public ControlledShutdownLeaderSelector(ControllerContext controllerContext) {
        this.controllerContext = controllerContext;
        this.logIdent = "<ControlledShutdownLeaderSelector>: ";
    }


    public Tuple<LeaderAndIsr, List<Integer>> selectLeader(TopicAndPartition topicAndPartition, LeaderAndIsr currentLeaderAndIsr) {
        Integer currentLeaderEpoch = currentLeaderAndIsr.leaderEpoch;
        Integer currentLeaderIsrZkPathVersion = currentLeaderAndIsr.zkVersion;

        Integer currentLeader = currentLeaderAndIsr.leader;

        List<Integer> assignedReplicas = controllerContext.partitionReplicaAssignment.get(topicAndPartition);
        Set<Integer> liveOrShuttingDownBrokerIds = controllerContext.liveOrShuttingDownBrokerIds();
        List<Integer> liveAssignedReplicas = Sc.filter(assignedReplicas, r -> liveOrShuttingDownBrokerIds.contains(r));

        List<Integer> newIsr = Sc.filter(currentLeaderAndIsr.isr, brokerId -> !controllerContext.shuttingDownBrokerIds.contains(brokerId));
        Integer newLeader = Sc.head(newIsr);
        if (newLeader != null) {
            debug(String.format("Partition %s : current leader = %d, new leader = %d", topicAndPartition, currentLeader, newLeader));
            return Tuple.of(new LeaderAndIsr(newLeader, currentLeaderEpoch + 1, newIsr, currentLeaderIsrZkPathVersion + 1),
                    liveAssignedReplicas);
        } else {
            throw new StateChangeFailedException(String.format("No other replicas in ISR %s for %s besides" +
                    " shutting down brokers %s", currentLeaderAndIsr.isr, topicAndPartition, controllerContext.shuttingDownBrokerIds));
        }
    }
}

/**
 * Essentially does nothing. Returns the current leader and ISR, and the current
 * set of replicas assigned to a given topic/partition.
 */
class NoOpLeaderSelector extends Logging implements PartitionLeaderSelector {
    public ControllerContext controllerContext;

    public NoOpLeaderSelector(ControllerContext controllerContext) {
        this.controllerContext = controllerContext;
        this.logIdent = "<NoOpLeaderSelector>: ";
    }


    public Tuple<LeaderAndIsr, List<Integer>> selectLeader(TopicAndPartition topicAndPartition, LeaderAndIsr currentLeaderAndIsr) {
        warn("I should never have been asked to perform leader election, returning the current LeaderAndIsr and replica assignment.");
        return Tuple.of(currentLeaderAndIsr, controllerContext.partitionReplicaAssignment.get(topicAndPartition));
    }
}
