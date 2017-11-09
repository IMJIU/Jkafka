package kafka.controller;


import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import kafka.cluster.Broker;
import kafka.common.StateChangeFailedException;
import kafka.controller.channel.CallbackBuilder;
import kafka.controller.channel.Callbacks;
import kafka.controller.ctrl.*;
import kafka.func.Tuple;
import kafka.log.TopicAndPartition;
import kafka.utils.*;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;

import static kafka.controller.ReplicaState.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class represents the state machine for replicas. It defines the states that a replica can be in, and
 * transitions to move the replica to another legal state. The different states that a replica can be in are -
 * 1. NewReplica        : The controller can create new replicas during partition reassignment. In this state, a
 * replica can only get become follower state change request.  Valid previous
 * state is NonExistentReplica
 * 2. OnlineReplica     : Once a replica is started and part of the assigned replicas for its partition, it is in this
 * state. In this state, it can get either become leader or become follower state change requests.
 * Valid previous state are NewReplica, OnlineReplica or OfflineReplica
 * 3. OfflineReplica    : If a replica dies, it moves to this state. This happens when the broker hosting the replica
 * is down. Valid previous state are NewReplica, OnlineReplica
 * 4. If ReplicaDeletionStarted replica deletion starts, it is moved to this state. Valid previous state is OfflineReplica
 * 5. If ReplicaDeletionSuccessful replica responds with no error code in response to a delete replica request, it is
 * moved to this state. Valid previous state is ReplicaDeletionStarted
 * 6. If ReplicaDeletionIneligible replica deletion fails, it is moved to this state. Valid previous state is ReplicaDeletionStarted
 * 7. If NonExistentReplica a replica is deleted successfully, it is moved to this state. Valid previous state is
 * ReplicaDeletionSuccessful
 */
public class ReplicaStateMachine extends Logging {
    KafkaController controller;
    private ControllerContext controllerContext;
    private Integer controllerId;
    private ZkClient zkClient;
    private Map<PartitionAndReplica, ReplicaState> replicaState = Maps.newHashMap();
    private BrokerChangeListener brokerChangeListener = new BrokerChangeListener();
    private ControllerBrokerRequestBatch brokerRequestBatch;
    private AtomicBoolean hasStarted = new AtomicBoolean(false);
    private StateChangeLogger stateChangeLogger = KafkaController.stateChangeLogger;

    public ReplicaStateMachine(KafkaController controller) {
        this.controller = controller;
        this.logIdent = "<Replica state machine on controller " + controller.config.brokerId + ">: ";
        controllerContext = controller.controllerContext;
        controllerId = controller.config.brokerId;
        zkClient = controllerContext.zkClient;
        brokerRequestBatch = new ControllerBrokerRequestBatch(controller);
    }


    /**
     * Invoked on successful controller election. First registers a broker change listener since that triggers all
     * state transitions for replicas. Initializes the state of replicas for all partitions by reading from zookeeper.
     * Then triggers the OnlineReplica state change for all replicas.
     */
    public void startup() {
        // initialize replica state;
        initializeReplicaState();
        // set started flag;
        hasStarted.set(true);
        // move all Online replicas to Online;
        handleStateChanges(controllerContext.allLiveReplicas(), OnlineReplica);

        info("Started replica state machine with initial state -> " + replicaState.toString());
    }

    // register ZK listeners of the replica state machine;
    public void registerListeners() {
        // register broker change listener;
        registerBrokerChangeListener();
    }

    // de-register ZK listeners of the replica state machine;
    public void deregisterListeners() {
        // de-register broker change listener;
        deregisterBrokerChangeListener();
    }

    /**
     * Invoked on controller shutdown.
     */
    public void shutdown() {
        // reset started flag;
        hasStarted.set(false);
        // reset replica state;
        replicaState.clear();
        // de-register all ZK listeners;
        deregisterListeners();

        info("Stopped replica state machine");
    }

    /**
     * This API is invoked by the broker change controller callbacks and the startup API of the state machine
     *
     * @param replicas    The list of replicas (brokers) that need to be transitioned to the target state
     * @param targetState The state that the replicas should be moved to
     *                    The controller's allLeaders cache should have been updated before this
     */
    public void handleStateChanges(Set<PartitionAndReplica> replicas, ReplicaState targetState) {
        this.handleStateChanges(replicas, targetState, (new CallbackBuilder()).build());
    }

    public void handleStateChanges(Set<PartitionAndReplica> replicas, ReplicaState targetState, Callbacks callbacks) {
        if (replicas.size() > 0) {
            info(String.format("Invoking state change to %s for replicas %s", targetState, replicas));
            try {
                brokerRequestBatch.newBatch();
                replicas.forEach(r -> handleStateChange(r, targetState, callbacks));
                brokerRequestBatch.sendRequestsToBrokers(controller.epoch(), controllerContext.correlationId.getAndIncrement());
            } catch (Throwable e) {
                error(String.format("Error while moving some replicas to %s state", targetState), e);
            }
        }
    }

    /**
     * This API exercises the replica's state machine. It ensures that every state transition happens from a legal
     * previous state to the target state. Valid state transitions are:
     * NonExistentReplica --> NewReplica
     * --send LeaderAndIsr request with current leader and isr to the new replica and UpdateMetadata request for the
     * partition to every live broker
     * <p>
     * NewReplica -> OnlineReplica
     * --add the new replica to the assigned replica list if needed
     * <p>
     * OnlineReplica,OfflineReplica -> OnlineReplica
     * --send LeaderAndIsr request with current leader and isr to the new replica and UpdateMetadata request for the
     * partition to every live broker
     * <p>
     * NewReplica,OnlineReplica,OfflineReplica,ReplicaDeletionIneligible -> OfflineReplica
     * --send StopReplicaRequest to the replica (w/o deletion)
     * --remove this replica from the isr and send LeaderAndIsr request (with new isr) to the leader replica and
     * UpdateMetadata request for the partition to every live broker.
     * <p>
     * OfflineReplica -> ReplicaDeletionStarted
     * --send StopReplicaRequest to the replica (with deletion)
     * <p>
     * ReplicaDeletionStarted -> ReplicaDeletionSuccessful
     * -- mark the state of the replica in the state machine
     * <p>
     * ReplicaDeletionStarted -> ReplicaDeletionIneligible
     * -- mark the state of the replica in the state machine
     * <p>
     * ReplicaDeletionSuccessful -> NonExistentReplica
     * -- remove the replica from the in memory partition replica assignment cache
     *
     * @param partitionAndReplica The replica for which the state transition is invoked
     * @param targetState         The end state that the replica should be moved to
     */
    public void handleStateChange(PartitionAndReplica partitionAndReplica, ReplicaState targetState, Callbacks callbacks) {
        String topic = partitionAndReplica.topic;
        Integer partition = partitionAndReplica.partition;
        Integer replicaId = partitionAndReplica.replica;
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        if (!hasStarted.get())
            throw new StateChangeFailedException(String.format("Controller %d epoch %d initiated state change of replica %d for partition %s " +
                            "to %s failed because replica state machine has not started",
                    controllerId, controller.epoch(), replicaId, topicAndPartition, targetState));
        ReplicaState currState = replicaState.getOrDefault(partitionAndReplica, NonExistentReplica);
        try {
            List<Integer> replicaAssignment = controllerContext.partitionReplicaAssignment.get(topicAndPartition);
            switch (targetState) {
                case NewReplica:
                    assertValidPreviousStates(partitionAndReplica, Lists.newArrayList(NonExistentReplica), targetState);
                    // start replica as a follower to the current leader for its partition;
                    Optional<LeaderIsrAndControllerEpoch> leaderIsrAndControllerEpochOpt = ReplicationUtils.getLeaderIsrAndEpochForPartition(zkClient, topic, partition);
                    if (leaderIsrAndControllerEpochOpt.isPresent()) {
                        LeaderIsrAndControllerEpoch leaderIsrAndControllerEpoch = leaderIsrAndControllerEpochOpt.get();
                        if (leaderIsrAndControllerEpoch.leaderAndIsr.leader == replicaId)
                            throw new StateChangeFailedException(String.format("Replica %d for partition %s cannot be moved to NewReplica",
                                    replicaId, topicAndPartition) + "state as it is being requested to become leader");
                        brokerRequestBatch.addLeaderAndIsrRequestForBrokers(Lists.newArrayList(replicaId),
                                topic, partition, leaderIsrAndControllerEpoch,
                                replicaAssignment);
                    } else {
                        // new leader request will be sent to this replica when one gets elected;
                    }
                    replicaState.put(partitionAndReplica, NewReplica);
                    stateChangeLogger.trace(String.format("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s",
                            controllerId, controller.epoch(), replicaId, topicAndPartition, currState,
                            targetState));
                    break;
                case ReplicaDeletionStarted:
                    assertValidPreviousStates(partitionAndReplica, Lists.newArrayList(OfflineReplica), targetState);
                    replicaState.put(partitionAndReplica, ReplicaDeletionStarted);
                    // send stop replica command;
                    brokerRequestBatch.addStopReplicaRequestForBrokers(Lists.newArrayList(replicaId), topic, partition, true, callbacks.stopReplicaResponseCallback);
                    stateChangeLogger.trace(String.format("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s",
                            controllerId, controller.epoch(), replicaId, topicAndPartition, currState, targetState));
                    break;
                case ReplicaDeletionIneligible:
                    assertValidPreviousStates(partitionAndReplica, Lists.newArrayList(ReplicaDeletionStarted), targetState);
                    replicaState.put(partitionAndReplica, ReplicaDeletionIneligible);
                    stateChangeLogger.trace(String.format("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s",
                            controllerId, controller.epoch(), replicaId, topicAndPartition, currState, targetState));
                    break;
                case ReplicaDeletionSuccessful:
                    assertValidPreviousStates(partitionAndReplica, Lists.newArrayList(ReplicaDeletionStarted), targetState);
                    replicaState.put(partitionAndReplica, ReplicaDeletionSuccessful);
                    stateChangeLogger.trace(String.format("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s",
                            controllerId, controller.epoch(), replicaId, topicAndPartition, currState, targetState));
                    break;
                case NonExistentReplica:
                    assertValidPreviousStates(partitionAndReplica, Lists.newArrayList(ReplicaDeletionSuccessful), targetState);
                    // remove this replica from the assigned replicas list for its partition;
                    List<Integer> currentAssignedReplicas = controllerContext.partitionReplicaAssignment.get(topicAndPartition);
                    controllerContext.partitionReplicaAssignment.put(topicAndPartition, Sc.filterNot(currentAssignedReplicas, r -> r == replicaId));
                    replicaState.remove(partitionAndReplica);
                    stateChangeLogger.trace(String.format("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s",
                            controllerId, controller.epoch(), replicaId, topicAndPartition, currState, targetState));
                    break;
                case OnlineReplica:
                    assertValidPreviousStates(partitionAndReplica,
                            Lists.newArrayList(NewReplica, OnlineReplica, OfflineReplica, ReplicaDeletionIneligible), targetState);
                    switch (replicaState.get(partitionAndReplica)) {
                        case NewReplica:
                            // add this replica to the assigned replicas list for its partition;
                            List<Integer> currentAssignedReplicas1 = controllerContext.partitionReplicaAssignment.get(topicAndPartition);
                            if (!currentAssignedReplicas1.contains(replicaId)) {
                                currentAssignedReplicas1.add(replicaId);
                                controllerContext.partitionReplicaAssignment.put(topicAndPartition, currentAssignedReplicas1);
                            }
                            stateChangeLogger.trace(String.format("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s",
                                    controllerId, controller.epoch(), replicaId, topicAndPartition, currState,
                                    targetState));
                        default:
                            // check if the leader for this partition ever existed;
                            LeaderIsrAndControllerEpoch leaderIsrAndControllerEpoch = controllerContext.partitionLeadershipInfo.get(topicAndPartition);
                            if (leaderIsrAndControllerEpoch != null) {
                                brokerRequestBatch.addLeaderAndIsrRequestForBrokers(Lists.newArrayList(replicaId), topic, partition, leaderIsrAndControllerEpoch,
                                        replicaAssignment);
                                replicaState.put(partitionAndReplica, OnlineReplica);
                                stateChangeLogger.trace(String.format("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s",
                                        controllerId, controller.epoch(), replicaId, topicAndPartition, currState, targetState));
                            } else { // that means the partition was never in OnlinePartition state, this means the broker never;
                                // started a log for that partition and does not have a high watermark value for this partition;
                            }
                    }
                    replicaState.put(partitionAndReplica, OnlineReplica);
                    break;
                case OfflineReplica:
                    assertValidPreviousStates(partitionAndReplica,
                            Lists.newArrayList(NewReplica, OnlineReplica, OfflineReplica, ReplicaDeletionIneligible), targetState);
                    // send stop replica command to the replica so that it stops fetching from the leader;
                    brokerRequestBatch.addStopReplicaRequestForBrokers(Lists.newArrayList(replicaId), topic, partition, false, null);
                    // As an optimization, the controller removes dead replicas from the ISR;
                    Boolean leaderAndIsrIsEmpty;
                    LeaderIsrAndControllerEpoch currLeaderIsrAndControllerEpoch = controllerContext.partitionLeadershipInfo.get(topicAndPartition);
                    if (currLeaderIsrAndControllerEpoch != null) {
                        Optional<LeaderIsrAndControllerEpoch> updatedLeaderIsrAndControllerEpochOpt = controller.removeReplicaFromIsr(topic, partition, replicaId);
                        if (updatedLeaderIsrAndControllerEpochOpt.isPresent()) {
                            LeaderIsrAndControllerEpoch updatedLeaderIsrAndControllerEpoch = updatedLeaderIsrAndControllerEpochOpt.get();
                            // send the shrunk ISR state change request to all the remaining alive replicas of the partition.;
                            List<Integer> currentAssignedReplicas2 = controllerContext.partitionReplicaAssignment.get(topicAndPartition);
                            if (!controller.deleteTopicManager.isPartitionToBeDeleted(topicAndPartition)) {
                                brokerRequestBatch.addLeaderAndIsrRequestForBrokers(Sc.filterNot(currentAssignedReplicas2, r -> r == replicaId),
                                        topic, partition, updatedLeaderIsrAndControllerEpoch, replicaAssignment);
                            }
                            replicaState.put(partitionAndReplica, OfflineReplica);
                            stateChangeLogger.trace(String.format("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s",
                                    controllerId, controller.epoch(), replicaId, topicAndPartition, currState, targetState));
                            leaderAndIsrIsEmpty = false;
                        } else {
                            leaderAndIsrIsEmpty = true;
                        }
                    } else {
                        leaderAndIsrIsEmpty = true;
                    }
                    if (leaderAndIsrIsEmpty)
                        throw new StateChangeFailedException(
                                String.format("Failed to change state of replica %d for partition %s since the leader and isr path in zookeeper is empty",
                                        replicaId, topicAndPartition));
            }
        } catch (Throwable t) {
            stateChangeLogger.error(String.format("Controller %d epoch %d initiated state change of replica %d for partition <%s,%d> from %s to %s failed",
                    controllerId, controller.epoch(), replicaId, topic, partition, currState, targetState), t);
        }
    }

    public Boolean areAllReplicasForTopicDeleted(String topic) {
        Set<PartitionAndReplica> replicasForTopic = controller.controllerContext.replicasForTopic(topic);
        Map<PartitionAndReplica, ReplicaState> replicaStatesForTopic = Sc.toMap(Sc.map(replicasForTopic, r -> Tuple.of(r, replicaState.get(r))));
        debug(String.format("Are all replicas for topic %s deleted %s", topic, replicaStatesForTopic));
        for (Map.Entry<PartitionAndReplica, ReplicaState> entry : replicaStatesForTopic.entrySet()) {
            if (entry.getValue() != ReplicaDeletionSuccessful) {
                return false;
            }
        }
        return true;
    }

    public Boolean isAtLeastOneReplicaInDeletionStartedState(String topic) {
        Set<PartitionAndReplica> replicasForTopic = controller.controllerContext.replicasForTopic(topic);
        Map<PartitionAndReplica, ReplicaState> replicaStatesForTopic = Sc.toMap(Sc.map(replicasForTopic, r -> Tuple.of(r, replicaState.get(r))));
        for (Map.Entry<PartitionAndReplica, ReplicaState> entry : replicaStatesForTopic.entrySet()) {
            if (entry.getValue() == ReplicaDeletionStarted) {
                return true;
            }
        }
        return false;
    }

    public Set<PartitionAndReplica> replicasInState(String topic, ReplicaState state) {
        return Sc.filter(replicaState, (r, s) -> r.topic.equals(topic) && s == state).keySet();
    }

    public Boolean isObjectReplicaInState(String topic, ReplicaState state) {
        return Sc.exists(replicaState, (r, s) -> r.topic.equals(topic) && s == state);
    }

    public Set<PartitionAndReplica> replicasInDeletionStates(String topic) {
        Set<ReplicaState> deletionStates = Sets.newHashSet(ReplicaDeletionStarted, ReplicaDeletionSuccessful, ReplicaDeletionIneligible);
        return Sc.filter(replicaState, (r, s) -> r.topic.equals(topic) && deletionStates.contains(s)).keySet();
    }

    private void assertValidPreviousStates(PartitionAndReplica partitionAndReplica, List<ReplicaState> fromStates, ReplicaState targetState) {
        Prediction.Assert(fromStates.contains(replicaState.get(partitionAndReplica)),
                String.format("Replica %s should be in the %s states before moving to %s state",
                        partitionAndReplica, fromStates, targetState) +
                        String.format(". Instead it is in %s state", replicaState.get(partitionAndReplica)));
    }

    private void registerBrokerChangeListener() {
        zkClient.subscribeChildChanges(ZkUtils.BrokerIdsPath, brokerChangeListener);
    }

    private void deregisterBrokerChangeListener() {
        zkClient.unsubscribeChildChanges(ZkUtils.BrokerIdsPath, brokerChangeListener);
    }

    /**
     * Invoked on startup of the replica's state machine to set the initial state for replicas of all existing partitions
     * in zookeeper
     */
    private void initializeReplicaState() {
        controllerContext.partitionReplicaAssignment.forEach((topicPartition, assignedReplicas) -> {
            String topic = topicPartition.topic;
            Integer partition = topicPartition.partition;
            assignedReplicas.forEach(replicaId -> {
                PartitionAndReplica partitionAndReplica = new PartitionAndReplica(topic, partition, replicaId);
                if (controllerContext.liveBrokerIds().contains(replicaId)) {
                    replicaState.put(partitionAndReplica, OnlineReplica);
                } else {
                    // mark replicas on dead brokers as failed for topic deletion, if they belong to a topic to be deleted.;
                    // This is required during controller failover since during controller failover a broker can go down,
                    // so the replicas on that broker should be moved to ReplicaDeletionIneligible to be on the safer side.;
                    replicaState.put(partitionAndReplica, ReplicaDeletionIneligible);
                }
            });
        });
    }

    public List<TopicAndPartition> partitionsAssignedToBroker(List<String> topics, Integer brokerId) {
        return Sc.toList(Sc.filter(controllerContext.partitionReplicaAssignment, (p, r) -> r.contains(brokerId)).keySet());
    }

    /**
     * This is the zookeeper listener that triggers all the state transitions for a replica
     */
    class BrokerChangeListener extends Logging implements IZkChildListener {
        public BrokerChangeListener() {
            this.logIdent = "<BrokerChangeListener on Controller " + controller.config.brokerId + ">: ";
        }

        public void handleChildChange(String parentPath, java.util.List<String> currentBrokerList) {
            info(String.format("Broker change listener fired for path %s with children %s", parentPath, currentBrokerList));
            Utils.inLock(controllerContext.controllerLock, () -> {
                if (hasStarted.get()) {
                    ControllerStats.leaderElectionTimer.time(() -> {
                        try {
                            List<Integer> curBrokerIds = Sc.map(currentBrokerList, b -> Integer.parseInt(b.toString()));
                            List<Integer> newBrokerIds = Lists.newArrayList();
                            curBrokerIds.forEach(b -> {
                                if (!controllerContext.liveOrShuttingDownBrokerIds().contains(b)) {
                                    newBrokerIds.add(b);
                                }
                            });
                            List<Optional<Broker>> newBrokerInfo = Sc.map(newBrokerIds, b -> ZkUtils.getBrokerInfo(zkClient, b));
                            List<Broker> newBrokers = Sc.map(Sc.filter(newBrokerInfo, b -> b.isPresent()), b2 -> b2.get());
                            Set<Integer> deadBrokerIds = Sets.newHashSet();
                            controllerContext.liveOrShuttingDownBrokerIds().forEach(b -> {
                                if (!curBrokerIds.contains(b)) {
                                    deadBrokerIds.add(b);
                                }
                            });
                            controllerContext.liveBrokers_(
                                    Sc.toSet(Sc.map(
                                            Sc.filter(
                                                    Sc.map(curBrokerIds, b -> ZkUtils.getBrokerInfo(zkClient, b))
                                                    , b2 -> b2.isPresent())
                                            , b3 -> b3.get())));
                            info(String.format("Newly added brokers: %s, deleted brokers: %s, all live brokers: %s",
                                    newBrokerIds, deadBrokerIds, controllerContext.liveBrokerIds()));
                            newBrokers.forEach(b -> controllerContext.controllerChannelManager.addBroker(b));
                            deadBrokerIds.forEach(b -> controllerContext.controllerChannelManager.removeBroker(b));
                            if (newBrokerIds.size() > 0)
                                controller.onBrokerStartup(newBrokerIds);
                            if (deadBrokerIds.size() > 0)
                                controller.onBrokerFailure(Sc.toList(deadBrokerIds));
                        } catch (Throwable e) {
                            error("Error while handling broker changes", e);
                        }
                        return null;
                    });
                }
            });
        }
    }
}


