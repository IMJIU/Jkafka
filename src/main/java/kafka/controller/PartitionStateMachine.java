package kafka.controller;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import kafka.api.LeaderAndIsr;
import kafka.common.LeaderElectionNotNeededException;
import kafka.common.NoReplicaOnlineException;
import kafka.common.StateChangeFailedException;
import kafka.controller.channel.CallbackBuilder;
import kafka.controller.channel.Callbacks;
import kafka.controller.ctrl.ControllerContext;
import kafka.controller.ctrl.LeaderIsrAndControllerEpoch;
import kafka.controller.partition.PartitionState;
import kafka.func.Tuple;
import kafka.log.TopicAndPartition;
import kafka.utils.*;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static kafka.controller.partition.PartitionState.*;

/**
 * This class represents the state machine for partitions. It defines the states that a partition can be in, and
 * transitions to move the partition to another legal state. The different states that a partition can be in are -
 * 1. This NonExistentPartition state indicates that the partition was either never created or was created and then
 * deleted. Valid previous state, if one exists, is OfflinePartition
 * 2. NewPartition        : After creation, the partition is in the NewPartition state. In this state, the partition should have
 * replicas assigned to it, but no leader/isr yet. Valid previous states are NonExistentPartition
 * 3. OnlinePartition     : Once a leader is elected for a partition, it is in the OnlinePartition state.
 * Valid previous states are NewPartition/OfflinePartition
 * 4. OfflinePartition    : If, after successful leader election, the leader for partition dies, then the partition
 * moves to the OfflinePartition state. Valid previous states are NewPartition/OnlinePartition
 */
public class PartitionStateMachine extends Logging {
    public KafkaController controller;

    public PartitionStateMachine(KafkaController controller) {
        this.controller = controller;
        this.logIdent = "<Partition state machine on Controller " + controllerId + ">: ";
        controllerContext = controller.controllerContext;
        controllerId = controller.config.brokerId;
        zkClient = controllerContext.zkClient;
        partitionState = Maps.newHashMap();
        brokerRequestBatch = new ControllerBrokerRequestBatch(controller);
        hasStarted = new AtomicBoolean(false);
        noOpPartitionLeaderSelector = new NoOpLeaderSelector(controllerContext);
        topicChangeListener = new TopicChangeListener();
        deleteTopicsListener = new DeleteTopicsListener();
        addPartitionsListener = Maps.newHashMap();
        stateChangeLogger = KafkaController.stateChangeLogger;
    }

    private ControllerContext controllerContext;
    private Integer controllerId;
    private ZkClient zkClient;
    private Map<TopicAndPartition, PartitionState> partitionState;
    private ControllerBrokerRequestBatch brokerRequestBatch;
    private AtomicBoolean hasStarted;
    private PartitionLeaderSelector noOpPartitionLeaderSelector;
    private TopicChangeListener topicChangeListener;
    private DeleteTopicsListener deleteTopicsListener;
    private Map<String, AddPartitionsListener> addPartitionsListener;
    private StateChangeLogger stateChangeLogger;


    /**
     * Invoked on successful controller election. First registers a topic change listener since that triggers all
     * state transitions for partitions. Initializes the state of partitions by reading from zookeeper. Then triggers
     * the OnlinePartition state change for all new or offline partitions.
     */
    public void startup() {
        // initialize partition state;
        initializePartitionState();
        // set started flag;
        hasStarted.set(true);
        // try to move partitions to online state;
        triggerOnlinePartitionStateChange();

        info("Started partition state machine with initial state -> " + partitionState.toString());
    }

    // register topic and partition change listeners;
    public void registerListeners() {
        registerTopicChangeListener();
        if (controller.config.deleteTopicEnable)
            registerDeleteTopicListener();
    }

    // de-register topic and partition change listeners;
    public void deregisterListeners() {
        deregisterTopicChangeListener();
        addPartitionsListener.forEach((topic, listener) ->
                zkClient.unsubscribeDataChanges(ZkUtils.getTopicPath(topic), listener));
        addPartitionsListener.clear();
        if (controller.config.deleteTopicEnable)
            deregisterDeleteTopicListener();
    }

    /**
     * Invoked on controller shutdown.
     */
    public void shutdown() {
        // reset started flag;
        hasStarted.set(false);
        // clear partition state;
        partitionState.clear();
        // de-register all ZK listeners;
        deregisterListeners();

        info("Stopped partition state machine");
    }

    /**
     * This API invokes the OnlinePartition state change on all partitions in either the NewPartition or OfflinePartition
     * state. This is called on a successful controller election and on broker changes
     */
    public void triggerOnlinePartitionStateChange() {
        try {
            brokerRequestBatch.newBatch();
            // try to move all partitions in NewPartition or OfflinePartition state to OnlinePartition state except partitions;
            // that belong to topics to be deleted;
            partitionState.forEach((topicAndPartition, partitionState) -> {
                if (!controller.deleteTopicManager.isTopicQueuedUpForDeletion(topicAndPartition.topic)) {
                    if (partitionState.equals(OfflinePartition) || partitionState.equals(NewPartition))
                        handleStateChange(topicAndPartition.topic, topicAndPartition.partition, OnlinePartition, controller.offlinePartitionSelector,
                                (new CallbackBuilder()).build());
                }
            });

            brokerRequestBatch.sendRequestsToBrokers(controller.epoch(), controllerContext.correlationId.getAndIncrement());
        } catch (Throwable e) {
            error("Error while moving some partitions to the online state", e);
            // It TODO is not enough to bail out and log an error, it is important to trigger leader election for those partitions;
        }
    }

    public Set<TopicAndPartition> partitionsInState(PartitionState state) {
        return Sc.filter(partitionState, (k, v) -> v == state).keySet();
    }

    /**
     * This API is invoked by the partition change zookeeper listener
     *
     * @param partitions  The list of partitions that need to be transitioned to the target state
     * @param targetState The state that the partitions should be moved to
     */
    public void handleStateChanges(Set<TopicAndPartition> partitions, PartitionState targetState, Callbacks callbacks) {
        handleStateChanges(partitions, targetState, noOpPartitionLeaderSelector, callbacks);
    }

    public void handleStateChanges(Set<TopicAndPartition> partitions, PartitionState targetState) {
        handleStateChanges(partitions, targetState, noOpPartitionLeaderSelector, new CallbackBuilder().build());
    }

    public void handleStateChanges(Set<TopicAndPartition> partitions, PartitionState targetState,
                                   PartitionLeaderSelector leaderSelector,
                                   Callbacks callbacks) {
        info(String.format("Invoking state change to %s for partitions %s", targetState, partitions));
        try {
            brokerRequestBatch.newBatch();
            partitions.forEach(topicAndPartition ->
                    handleStateChange(topicAndPartition.topic, topicAndPartition.partition, targetState, leaderSelector, callbacks));
            brokerRequestBatch.sendRequestsToBrokers(controller.epoch(), controllerContext.correlationId.getAndIncrement());
        } catch (Throwable e) {
            error(String.format("Error while moving some partitions to %s state", targetState), e);
            // It TODO is not enough to bail out and log an error, it is important to trigger state changes for those partitions;
        }
    }

    /**
     * This API exercises the partition's state machine. It ensures that every state transition happens from a legal
     * previous state to the target state. Valid state transitions are:
     * NonExistentPartition -> NewPartition:
     * --load assigned replicas from ZK to controller cache
     * <p>
     * NewPartition -> OnlinePartition
     * --assign first live replica as the leader and all live replicas as the isr; write leader and isr to ZK for this partition
     * --send LeaderAndIsr request to every live replica and UpdateMetadata request to every live broker
     * <p>
     * OnlinePartition,OfflinePartition -> OnlinePartition
     * --select new leader and isr for this partition and a set of replicas to receive the LeaderAndIsr request, and write leader and isr to ZK
     * --for this partition, send LeaderAndIsr request to every receiving replica and UpdateMetadata request to every live broker
     * <p>
     * NewPartition,OnlinePartition,OfflinePartition -> OfflinePartition
     * --nothing other than marking partition state as Offline
     * <p>
     * OfflinePartition -> NonExistentPartition
     * --nothing other than marking the partition state as NonExistentPartition
     *
     * @param topic       The topic of the partition for which the state transition is invoked
     * @param partition   The partition for which the state transition is invoked
     * @param targetState The end state that the partition should be moved to
     */
    private void handleStateChange(String topic, Integer partition, PartitionState targetState, PartitionLeaderSelector leaderSelector, Callbacks callbacks) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        if (!hasStarted.get())
            throw new StateChangeFailedException(String.format("Controller %d epoch %d initiated state change for partition %s to %s failed because " +
                    "the partition state machine has not started", controllerId, controller.epoch(), topicAndPartition, targetState));
        PartitionState currState = partitionState.getOrDefault(topicAndPartition, NonExistentPartition);
        try {
            switch (targetState) {
                case NewPartition:
                    // partition pre did not exist before this;
                    assertValidPreviousStates(topicAndPartition, Lists.newArrayList(NonExistentPartition), NewPartition);
                    assignReplicasToPartitions(topic, partition);
                    partitionState.put(topicAndPartition, NewPartition);
                    List<Integer> assignedReplicas = controllerContext.partitionReplicaAssignment.get(topicAndPartition);
                    stateChangeLogger.trace(String.format("Controller %d epoch %d changed partition %s state from %s to %s with assigned replicas %s",
                            controllerId, controller.epoch(), topicAndPartition, currState, targetState, assignedReplicas));
                    // partition post has been assigned replicas;
                    break;
                case OnlinePartition:
                    assertValidPreviousStates(topicAndPartition, Lists.newArrayList(NewPartition, OnlinePartition, OfflinePartition), OnlinePartition);
                    switch (partitionState.get(topicAndPartition)) {
                        case NewPartition:
                            // initialize leader and isr path for new partition;
                            initializeLeaderAndIsrForPartition(topicAndPartition);
                            break;
                        case OfflinePartition:
                            electLeaderForPartition(topic, partition, leaderSelector);
                            break;
                        case OnlinePartition: // invoked when the leader needs to be re-elected;
                            electLeaderForPartition(topic, partition, leaderSelector);
                            break;
                        default: // should never come here since illegal previous states are checked above;
                    }
                    partitionState.put(topicAndPartition, OnlinePartition);
                    Integer leader = controllerContext.partitionLeadershipInfo.get(topicAndPartition).leaderAndIsr.leader;
                    stateChangeLogger.trace(String.format("Controller %d epoch %d changed partition %s from %s to %s with leader %d",
                            controllerId, controller.epoch(), topicAndPartition, currState, targetState, leader));
                    break;
                // partition post has a leader;

                case OfflinePartition:
                    // partition pre should be in New or Online state;
                    assertValidPreviousStates(topicAndPartition, Lists.newArrayList(NewPartition, OnlinePartition, OfflinePartition), OfflinePartition);
                    // should be called when the leader for a partition is no longer alive;
                    stateChangeLogger.trace(String.format("Controller %d epoch %d changed partition %s state from %s to %s",
                            controllerId, controller.epoch(), topicAndPartition, currState, targetState));
                    partitionState.put(topicAndPartition, OfflinePartition);
                    break;
                // partition post has no alive leader;
                case NonExistentPartition:
                    // partition pre should be in Offline state;
                    assertValidPreviousStates(topicAndPartition, Lists.newArrayList(OfflinePartition), NonExistentPartition);
                    stateChangeLogger.trace(String.format("Controller %d epoch %d changed partition %s state from %s to %s",
                            controllerId, controller.epoch(), topicAndPartition, currState, targetState));
                    partitionState.put(topicAndPartition, NonExistentPartition);
                    // partition post state is deleted from all brokers and zookeeper;
            }
        } catch (Throwable t) {
            stateChangeLogger.error(String.format("Controller %d epoch %d initiated state change for partition %s from %s to %s failed",
                    controllerId, controller.epoch(), topicAndPartition, currState, targetState), t);
        }
    }

    /**
     * Invoked on startup of the partition's state machine to set the initial state for all existing partitions in
     * zookeeper
     */
    private void initializePartitionState() {
        controllerContext.partitionReplicaAssignment.forEach((topicPartition, replicaAssignment) -> {
            // check if leader and isr path exists for partition. If not, then it is in NEW state;
            kafka.controller.ctrl.LeaderIsrAndControllerEpoch currentLeaderIsrAndEpoch = controllerContext.partitionLeadershipInfo.get(topicPartition);
            if (currentLeaderIsrAndEpoch != null) {
                // else, check if the leader for partition is alive. If yes, it is in Online state, else it is in Offline state;
                if (controllerContext.liveBrokerIds().contains(currentLeaderIsrAndEpoch.leaderAndIsr.leader)) {
                    // leader is alive;
                    partitionState.put(topicPartition, OnlinePartition);
                } else {
                    partitionState.put(topicPartition, OfflinePartition);
                }
            } else {
                partitionState.put(topicPartition, NewPartition);
            }
        });
    }

    private void assertValidPreviousStates(TopicAndPartition topicAndPartition, List<PartitionState> fromStates,
                                           PartitionState targetState) {
        if (!fromStates.contains(partitionState.get(topicAndPartition)))
            throw new IllegalStateException(String.format("Partition %s should be in the %s states before moving to %s state",
                    topicAndPartition, fromStates, targetState) + String.format(". Instead it is in %s state", partitionState.get(topicAndPartition)));
    }

    /**
     * Invoked on the NonExistentPartition->NewPartition state transition to update the controller's cache with the
     * partition's replica assignment.
     *
     * @param topic     The topic of the partition whose replica assignment is to be cached
     * @param partition The partition whose replica assignment is to be cached
     */
    private void assignReplicasToPartitions(String topic, Integer partition) {
        List<Integer> assignedReplicas = ZkUtils.getReplicasForPartition(controllerContext.zkClient, topic, partition);
        controllerContext.partitionReplicaAssignment.put(new TopicAndPartition(topic, partition), assignedReplicas);
    }

    /**
     * Invoked on the NewPartition->OnlinePartition state change. When a partition is in the New state, it does not have
     * a leader and isr path in zookeeper. Once the partition moves to the OnlinePartition state, it's leader and isr
     * path gets initialized and it never goes back to the NewPartition state. From here, it can only go to the
     * OfflinePartition state.
     *
     * @param topicAndPartition The topic/partition whose leader and isr path is to be initialized
     */
    private void initializeLeaderAndIsrForPartition(TopicAndPartition topicAndPartition) {
        List<Integer> replicaAssignment = controllerContext.partitionReplicaAssignment.get(topicAndPartition);
        List<Integer> liveAssignedReplicas = Sc.filter(replicaAssignment, r -> controllerContext.liveBrokerIds().contains(r));
        if (liveAssignedReplicas.size() == 0) {
            String failMsg = String.format("encountered error during state change of partition %s from New to Online, assigned replicas are <%s>, " +
                            "live brokers are <%s>. No assigned replica is alive.",
                    topicAndPartition, replicaAssignment, controllerContext.liveBrokerIds());
            stateChangeLogger.error(String.format("Controller %d epoch %d ", controllerId, controller.epoch()) + failMsg);
            throw new StateChangeFailedException(failMsg);
        } else {
            debug(String.format("Live assigned replicas for partition %s are: <%s>", topicAndPartition, liveAssignedReplicas));
            // make the first replica in the list of assigned replicas, the leader;
            Integer leader = Sc.head(liveAssignedReplicas);
            LeaderIsrAndControllerEpoch leaderIsrAndControllerEpoch = new LeaderIsrAndControllerEpoch(new LeaderAndIsr(leader, liveAssignedReplicas),
                    controller.epoch());
            debug(String.format("Initializing leader and isr for partition %s to %s", topicAndPartition, leaderIsrAndControllerEpoch));

            try {
                ZkUtils.createPersistentPath(controllerContext.zkClient,
                        ZkUtils.getTopicPartitionLeaderAndIsrPath(topicAndPartition.topic, topicAndPartition.partition),
                        ZkUtils.leaderAndIsrZkData(leaderIsrAndControllerEpoch.leaderAndIsr, controller.epoch()));
                // the NOTE above write can fail only if the current controller lost its zk session and the new controller;
                // took over and initialized this partition. This can happen if the current controller went into a long;
                // GC pause;
                controllerContext.partitionLeadershipInfo.put(topicAndPartition, leaderIsrAndControllerEpoch);
                brokerRequestBatch.addLeaderAndIsrRequestForBrokers(liveAssignedReplicas, topicAndPartition.topic,
                        topicAndPartition.partition, leaderIsrAndControllerEpoch, replicaAssignment);
            } catch (ZkNodeExistsException e) {
                // read the controller epoch;
                LeaderIsrAndControllerEpoch leaderIsrAndEpoch = ReplicationUtils.getLeaderIsrAndEpochForPartition(zkClient, topicAndPartition.topic,
                        topicAndPartition.partition).get();
                String failMsg = String.format("encountered error while changing partition %s's state from New to Online since LeaderAndIsr path already " +
                                "exists with value %s and controller epoch %d",
                        topicAndPartition, leaderIsrAndEpoch.leaderAndIsr.toString(), leaderIsrAndEpoch.controllerEpoch);
                stateChangeLogger.error(String.format("Controller %d epoch %d ", controllerId, controller.epoch()) + failMsg);
                throw new StateChangeFailedException(failMsg);
            }

        }
    }

    /**
     * Invoked on the OfflinePartition,OnlinePartition->OnlinePartition state change.
     * It invokes the leader election API to elect a leader for the input offline partition
     *
     * @param topic          The topic of the offline partition
     * @param partition      The offline partition
     * @param leaderSelector Specific leader selector (e.g., offline/reassigned/etc.)
     */
    public void electLeaderForPartition(String topic, Integer partition, PartitionLeaderSelector leaderSelector) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        // handle leader election for the partitions whose leader is no longer alive;
        stateChangeLogger.trace(String.format("Controller %d epoch %d started leader election for partition %s",
                controllerId, controller.epoch(), topicAndPartition));
        try {
            Boolean zookeeperPathUpdateSucceeded = false;
            LeaderAndIsr newLeaderAndIsr = null;
            List<Integer> replicasForThisPartition = Lists.newArrayList();
            while (!zookeeperPathUpdateSucceeded) {
                LeaderIsrAndControllerEpoch currentLeaderIsrAndEpoch = getLeaderIsrAndEpochOrThrowException(topic, partition);
                LeaderAndIsr currentLeaderAndIsr = currentLeaderIsrAndEpoch.leaderAndIsr;
                Integer controllerEpoch = currentLeaderIsrAndEpoch.controllerEpoch;
                if (controllerEpoch > controller.epoch()) {
                    String failMsg = String.format("aborted leader election for partition <%s,%d> since the LeaderAndIsr path was " +
                                    "already written by another controller. This probably means that the current controller %d went through " +
                                    "a soft failure and another controller was elected with epoch %d.",
                            topic, partition, controllerId, controllerEpoch);
                    stateChangeLogger.error(String.format("Controller %d epoch %d ", controllerId, controller.epoch()) + failMsg);
                    throw new StateChangeFailedException(failMsg);
                }
                // elect new leader or throw exception;
                Tuple<LeaderAndIsr, List<Integer>> t1 = leaderSelector.selectLeader(topicAndPartition, currentLeaderAndIsr);
                LeaderAndIsr leaderAndIsr = t1.v1;
                List<Integer> replicas = t1.v2;
                Tuple<Boolean, Integer> t2 = ReplicationUtils.updateLeaderAndIsr(zkClient, topic, partition,
                        leaderAndIsr, controller.epoch(), currentLeaderAndIsr.zkVersion);
                Boolean updateSucceeded = t2.v1;
                Integer newVersion = t2.v2;
                newLeaderAndIsr = leaderAndIsr;
                newLeaderAndIsr.zkVersion = newVersion;
                zookeeperPathUpdateSucceeded = updateSucceeded;
                replicasForThisPartition = replicas;
            }
            LeaderIsrAndControllerEpoch newLeaderIsrAndControllerEpoch = new LeaderIsrAndControllerEpoch(newLeaderAndIsr, controller.epoch());
            // update the leader cache;
            controllerContext.partitionLeadershipInfo.put(new TopicAndPartition(topic, partition), newLeaderIsrAndControllerEpoch);
            stateChangeLogger.trace(String.format("Controller %d epoch %d elected leader %d for Offline partition %s",
                    controllerId, controller.epoch(), newLeaderAndIsr.leader, topicAndPartition));
            List<Integer> replicas = controllerContext.partitionReplicaAssignment.get(new TopicAndPartition(topic, partition));
            // store new leader and isr info in cache;
            brokerRequestBatch.addLeaderAndIsrRequestForBrokers(replicasForThisPartition, topic, partition,
                    newLeaderIsrAndControllerEpoch, replicas);
        } catch (LeaderElectionNotNeededException lenne) { // swallow;
        } catch (NoReplicaOnlineException nroe) { throw nroe;} catch (Throwable sce) {
            String failMsg = String.format("encountered error while electing leader for partition %s due to: %s.", topicAndPartition, sce.getMessage());
            stateChangeLogger.error(String.format("Controller %d epoch %d ", controllerId, controller.epoch()) + failMsg);
            throw new StateChangeFailedException(failMsg, sce);
        }
        debug(String.format("After leader election, leader cache is updated to %s", controllerContext.partitionLeadershipInfo));
    }

    private void registerTopicChangeListener() {
        zkClient.subscribeChildChanges(ZkUtils.BrokerTopicsPath, topicChangeListener);
    }

    private void deregisterTopicChangeListener() {
        zkClient.unsubscribeChildChanges(ZkUtils.BrokerTopicsPath, topicChangeListener);
    }

    public void registerPartitionChangeListener(String topic) {
        addPartitionsListener.put(topic, new AddPartitionsListener(topic));
        zkClient.subscribeDataChanges(ZkUtils.getTopicPath(topic), addPartitionsListener.get(topic));
    }

    public void deregisterPartitionChangeListener(String topic) {
        zkClient.unsubscribeDataChanges(ZkUtils.getTopicPath(topic), addPartitionsListener.get(topic));
        addPartitionsListener.remove(topic);
    }

    private void registerDeleteTopicListener() {
        zkClient.subscribeChildChanges(ZkUtils.DeleteTopicsPath, deleteTopicsListener);
    }

    private void deregisterDeleteTopicListener() {
        zkClient.unsubscribeChildChanges(ZkUtils.DeleteTopicsPath, deleteTopicsListener);
    }

    private LeaderIsrAndControllerEpoch getLeaderIsrAndEpochOrThrowException(String topic, Integer partition) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Optional<LeaderIsrAndControllerEpoch> currentLeaderIsrAndEpochOpt = ReplicationUtils.getLeaderIsrAndEpochForPartition(zkClient, topic, partition);
        if (currentLeaderIsrAndEpochOpt.isPresent()) {
            return currentLeaderIsrAndEpochOpt.get();
        } else {
            String failMsg = String.format("LeaderAndIsr information doesn't exist for partition %s in %s state",
                    topicAndPartition, partitionState.get(topicAndPartition));
            throw new StateChangeFailedException(failMsg);
        }
    }

    /**
     * This is the zookeeper listener that triggers all the state transitions for a partition
     */
    class TopicChangeListener extends Logging implements IZkChildListener {
        public TopicChangeListener() {
            this.logIdent = "<TopicChangeListener on Controller " + controller.config.brokerId + ">: ";
        }

        //            @throws(classOf<Exception>)
        public void handleChildChange(String parentPath, java.util.List<String> children) {
            Utils.inLock(controllerContext.controllerLock, () -> {
                if (hasStarted.get()) {
                    try {
                        debug(String.format("Topic change listener fired for path %s with children %s", parentPath, children));
                        Set<String> currentChildren = Sc.toSet(children);
                        Set<String> newTopics = Sets.newHashSet();
                        currentChildren.forEach(c -> {
                            if (!controllerContext.allTopics.contains(c)) {
                                newTopics.add(c);
                            }
                        });
                        Set<String> deletedTopics = Sets.newHashSet();
                        controllerContext.allTopics.forEach(c -> {
                            if (!currentChildren.contains(c)) {
                                deletedTopics.add(c);
                            }
                        });

                        controllerContext.allTopics = currentChildren;

                        Map<TopicAndPartition, List<Integer>> addedPartitionReplicaAssignment = ZkUtils.getReplicaAssignmentForTopics(zkClient, Sc.toList(newTopics));
                        controllerContext.partitionReplicaAssignment = Sc.filter(controllerContext.partitionReplicaAssignment,
                                (k, v) -> !deletedTopics.contains(k.topic));
                        controllerContext.partitionReplicaAssignment.putAll(addedPartitionReplicaAssignment);
                        info(String.format("New topics: <%s], deleted topics: <%s>, new partition replica assignment [%s>", newTopics,
                                deletedTopics, addedPartitionReplicaAssignment));
                        if (newTopics.size() > 0)
                            controller.onNewTopicCreation(newTopics, addedPartitionReplicaAssignment.keySet());
                    } catch (Throwable e) {
                        error("Error while handling new topic", e);
                    }
                }
            });
        }
    }

    /**
     * Delete topics includes the following operations -
     * 1. Add the topic to be deleted to the delete topics cache, only if the topic exists
     * 2. If there are topics to be deleted, it signals the delete topic thread
     */
    class DeleteTopicsListener extends Logging implements IZkChildListener {
        public DeleteTopicsListener() {
            this.logIdent = "<DeleteTopicsListener on " + controller.config.brokerId + ">: ";
            zkClient = controllerContext.zkClient;
        }

        public ZkClient zkClient;

        /**
         * Invoked when a topic is being deleted
         *
         * @throws Exception On any error.
         */
//        @throws(classOf<Exception>)
        public void handleChildChange(String parentPath, java.util.List<String> children) {
            Utils.inLock(controllerContext.controllerLock, () -> {
                Set<String> topicsToBeDeleted = Sc.toSet(children);
                debug(String.format("Delete topics listener fired for topics %s to be deleted", topicsToBeDeleted));
                Set<String> nonExistentTopics = Sc.filter(topicsToBeDeleted, t -> !controllerContext.allTopics.contains(t));
                if (nonExistentTopics.size() > 0) {
                    warn("Ignoring request to delete non-existing topics " + nonExistentTopics);
                    nonExistentTopics.forEach(topic -> ZkUtils.deletePathRecursive(zkClient, ZkUtils.getDeleteTopicPath(topic)));
                }
                nonExistentTopics.forEach(t -> topicsToBeDeleted.remove(t));
                if (topicsToBeDeleted.size() > 0) {
                    info("Starting topic deletion for topics " + topicsToBeDeleted);
                    // mark topic ineligible for deletion if other state changes are in progress;
                    topicsToBeDeleted.forEach(topic -> {
                        boolean preferredReplicaElectionInProgress = Sc.map(controllerContext.partitionsUndergoingPreferredReplicaElection, p -> p.topic).contains(topic);
                        boolean partitionReassignmentInProgress =
                                Sc.map(controllerContext.partitionsBeingReassigned.keySet(), p -> p.topic).contains(topic);
                        if (preferredReplicaElectionInProgress || partitionReassignmentInProgress)
                            controller.deleteTopicManager.markTopicIneligibleForDeletion(Sets.newHashSet(topic));
                    });
                    // add topic to deletion list;
                    controller.deleteTopicManager.enqueueTopicsForDeletion(topicsToBeDeleted);
                }
            });
        }

        /**
         * @throws Exception On any error.
         */
//        @throws(classOf<Exception>)
        public void handleDataDeleted(String dataPath) {
        }
    }

    class AddPartitionsListener extends Logging implements IZkDataListener {
        String topic;

        public AddPartitionsListener(String topic) {
            this.topic = topic;
            this.logIdent = "<AddPartitionsListener on " + controller.config.brokerId + ">: ";
        }

//        @throws(classOf<Exception>)

        public void handleDataChange(String dataPath, Object data) {
            Utils.inLock(controllerContext.controllerLock, () -> {
                try {
                    info("Add Partition triggered " + data.toString() + " for path " + dataPath);
                    Map<TopicAndPartition, List<Integer>> partitionReplicaAssignment = ZkUtils.getReplicaAssignmentForTopics(zkClient, Lists.newArrayList(topic));
                    Map<TopicAndPartition, List<Integer>> partitionsToBeAdded = Sc.filter(partitionReplicaAssignment, (k, v) -> !controllerContext.partitionReplicaAssignment.containsKey(k));
                    if (controller.deleteTopicManager.isTopicQueuedUpForDeletion(topic))
                        error(String.format("Skipping adding partitions %s for topic %s since it is currently being deleted", Sc.map(partitionsToBeAdded, (k, v) -> k.partition), topic));
                    else {
                        if (partitionsToBeAdded.size() > 0) {
                            info(String.format("New partitions to be added %s", partitionsToBeAdded));
                            controller.onNewPartitionCreation(partitionsToBeAdded.keySet());
                        }
                    }
                } catch (Throwable e) {
                    error("Error while handling add partitions for data path " + dataPath, e);
                }
            });
        }

        //        @throws(classOf<Exception>)
        public void handleDataDeleted(String parentPath) {
            // this is not implemented for partition change;
        }
    }
}


