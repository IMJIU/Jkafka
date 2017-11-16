package kafka.controller;


import com.google.common.collect.Sets;
import kafka.api.RequestOrResponse;
import kafka.api.StopReplicaResponse;
import kafka.common.ErrorMapping;
import kafka.controller.channel.CallbackBuilder;
import kafka.controller.ctrl.ControllerContext;
import kafka.controller.ctrl.PartitionAndReplica;
import kafka.func.ActionP2;
import kafka.log.TopicAndPartition;
import kafka.utils.*;
import org.I0Itec.zkclient.ZkClient;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import static kafka.controller.ReplicaState.*;
import static kafka.controller.partition.PartitionState.NonExistentPartition;
import static kafka.controller.partition.PartitionState.OfflinePartition;

/**
 * This manages the state machine for topic deletion.
 * 1. TopicCommand issues topic deletion by creating a new admin path /admin/delete_topics/<topic>
 * 2. The controller listens for child changes on /admin/delete_topic and starts topic deletion for the respective topics
 * 3. The controller has a background thread that handles topic deletion. The purpose of having this background thread
 * is to accommodate the TTL feature, when we have it. This thread is signaled whenever deletion for a topic needs to
 * be started or resumed. Currently, a topic's deletion can be started only by the onPartitionDeletion callback on the
 * controller. In the future, it can be triggered based on the configured TTL for the topic. A topic will be ineligible
 * for deletion in the following scenarios -
 * 3.1 broker hosting one of the replicas for that topic goes down
 * 3.2 partition reassignment for partitions of that topic is in progress
 * 3.3 preferred replica election for partitions of that topic is in progress
 * (though this is not strictly required since it holds the controller lock for the entire duration from start to end)
 * 4. Topic deletion is resumed when -
 * 4.1 broker hosting one of the replicas for that topic is started
 * 4.2 preferred replica election for partitions of that topic completes
 * 4.3 partition reassignment for partitions of that topic completes
 * 5. Every replica for a topic being deleted is in either of the 3 states -
 * 5.1 TopicDeletionStarted (Replica enters TopicDeletionStarted phase when the onPartitionDeletion callback is invoked.
 * This happens when the child change watch for /admin/delete_topics fires on the controller. As part of this state
 * change, the controller sends StopReplicaRequests to all replicas. It registers a callback for the
 * StopReplicaResponse when deletePartition=true thereby invoking a callback when a response for delete replica
 * is received from every replica)
 * 5.2 TopicDeletionSuccessful (deleteTopicStopReplicaCallback() moves replicas from
 * TopicDeletionStarted->TopicDeletionSuccessful depending on the error codes in StopReplicaResponse)
 * 5.3 TopicDeletionFailed. (deleteTopicStopReplicaCallback() moves replicas from
 * TopicDeletionStarted->TopicDeletionFailed depending on the error codes in StopReplicaResponse.
 * In general, if a broker dies and if it hosted replicas for topics being deleted, the controller marks the
 * respective replicas in TopicDeletionFailed state in the onBrokerFailure callback. The reason is that if a
 * broker fails before the request is sent and after the replica is in TopicDeletionStarted state,
 * it is possible that the replica will mistakenly remain in TopicDeletionStarted state and topic deletion
 * will not be retried when the broker comes back up.)
 * 6. The delete topic thread marks a topic successfully deleted only if all replicas are in TopicDeletionSuccessful
 * state and it starts the topic deletion teardown mode where it deletes all topic state from the controllerContext
 * as well as from zookeeper. This is the only time the /brokers/topics/<topic> path gets deleted. On the other hand,
 * if no replica is in TopicDeletionStarted state and at least one replica is in TopicDeletionFailed state, then
 * it marks the topic for deletion retry.
 *
 * param controller
 * param initialTopicsToBeDeleted           The topics that are queued up for deletion in zookeeper at the time of controller failover
 * param initialTopicsIneligibleForDeletion The topics ineligible for deletion due to any of the conditions mentioned in #3 above
 */
public class TopicDeletionManager extends Logging {
    public KafkaController controller;
    public Set<String> initialTopicsToBeDeleted;
    public Set<String> initialTopicsIneligibleForDeletion;

    public TopicDeletionManager(KafkaController controller, Set<String> initialTopicsToBeDeleted, Set<String> initialTopicsIneligibleForDeletion) {
        this.controller = controller;
        this.initialTopicsToBeDeleted = initialTopicsToBeDeleted;
        this.initialTopicsIneligibleForDeletion = initialTopicsIneligibleForDeletion;
        this.logIdent = "<Topic Deletion Manager " + controller.config.brokerId + ">, ";
        controllerContext = controller.controllerContext;
        partitionStateMachine = controller.partitionStateMachine;
        replicaStateMachine = controller.replicaStateMachine;
        topicsToBeDeleted = Sets.newHashSet(initialTopicsToBeDeleted);
        partitionsToBeDeleted = Sc.flatMap(topicsToBeDeleted, t -> controllerContext.partitionsForTopic(t));
        deleteLock = new ReentrantLock();
        topicsIneligibleForDeletion = Sc.add(initialTopicsIneligibleForDeletion, initialTopicsToBeDeleted);
        deleteTopicsCond = deleteLock.newCondition();
        isDeleteTopicEnabled = controller.config.deleteTopicEnable;
    }

    public ControllerContext controllerContext;
    public PartitionStateMachine partitionStateMachine;
    public ReplicaStateMachine replicaStateMachine;
    public Set<String> topicsToBeDeleted;
    public Set<TopicAndPartition> partitionsToBeDeleted;
    public ReentrantLock deleteLock = new ReentrantLock();
    public Set<String> topicsIneligibleForDeletion;
    public Condition deleteTopicsCond;
    public AtomicBoolean deleteTopicStateChanged = new AtomicBoolean(false);
    public DeleteTopicsThread deleteTopicsThread = null;
    public Boolean isDeleteTopicEnabled;

    /**
     * Invoked at the end of new controller initiation
     */
    public void start() {
        if (isDeleteTopicEnabled) {
            deleteTopicsThread = new DeleteTopicsThread();
            if (topicsToBeDeleted.size() > 0)
                deleteTopicStateChanged.set(true);
            deleteTopicsThread.start();
        }
    }

    /**
     * Invoked when the current controller resigns. At this time, all state for topic deletion should be cleared.
     */
    public void shutdown() {
        // Only allow one shutdown to go through;
        if (isDeleteTopicEnabled && deleteTopicsThread.initiateShutdown()) {
            // Resume the topic deletion so it doesn't block on the condition;
            resumeTopicDeletionThread();
            // Await delete topic thread to exit;
            deleteTopicsThread.awaitShutdown();
            topicsToBeDeleted.clear();
            partitionsToBeDeleted.clear();
            topicsIneligibleForDeletion.clear();
        }
    }

    /**
     * Invoked by the child change listener on /admin/delete_topics to queue up the topics for deletion. The topic gets added
     * to the topicsToBeDeleted list and only gets removed from the list when the topic deletion has completed successfully
     * i.e. all replicas of all partitions of that topic are deleted successfully.
     *
     * @param topics Topics that should be deleted
     */
    public void enqueueTopicsForDeletion(Set<String> topics) {
        if (isDeleteTopicEnabled) {
            topicsToBeDeleted.addAll(topics);
            partitionsToBeDeleted.addAll(Sc.flatMap(topics, t -> controllerContext.partitionsForTopic(t)));
            resumeTopicDeletionThread();
        }
    }

    /**
     * Invoked when any event that can possibly resume topic deletion occurs. These events include -
     * 1. New broker starts up. Object replicas belonging to topics queued up for deletion can be deleted since the broker is up
     * 2. Partition reassignment completes. Object partitions belonging to topics queued up for deletion finished reassignment
     * 3. Preferred replica election completes. Object partitions belonging to topics queued up for deletion finished
     * preferred replica election
     *
     * param topics Topics for which deletion can be resumed
     */
    public void resumeDeletionForTopics() {

    }

    public void resumeDeletionForTopics(Set<String> topics) {
        if (isDeleteTopicEnabled) {
            Set<String> topicsToResumeDeletion = Sc.and(topics, topicsToBeDeleted);
            if (topicsToResumeDeletion.size() > 0) {
                topicsIneligibleForDeletion.removeAll(topicsToResumeDeletion);
                resumeTopicDeletionThread();
            }
        }
    }

    /**
     * Invoked when a broker that hosts replicas for topics to be deleted goes down. Also invoked when the callback for
     * StopReplicaResponse receives an error code for the replicas of a topic to be deleted. As part of this, the replicas
     * are moved from ReplicaDeletionStarted to ReplicaDeletionIneligible state. Also, the topic is added to the list of topics
     * ineligible for deletion until further notice. The delete topic thread is notified so it can retry topic deletion
     * if it has received a response for all replicas of a topic to be deleted
     *
     * @param replicas Replicas for which deletion has failed
     */
    public void failReplicaDeletion(Set<PartitionAndReplica> replicas) {
        if (isDeleteTopicEnabled) {
            Set<PartitionAndReplica> replicasThatFailedToDelete = Sc.filter(replicas, r -> isTopicQueuedUpForDeletion(r.topic));
            if (replicasThatFailedToDelete.size() > 0) {
                Set<String> topics = Sc.map(replicasThatFailedToDelete, r -> r.topic);
                debug(String.format("Deletion failed for replicas %s. Halting deletion for topics %s", replicasThatFailedToDelete, topics));
                controller.replicaStateMachine.handleStateChanges(replicasThatFailedToDelete, ReplicaDeletionIneligible);
                markTopicIneligibleForDeletion(topics);
                resumeTopicDeletionThread();
            }
        }
    }

    /**
     * Halt delete topic if -
     * 1. replicas being down
     * 2. partition reassignment in progress for some partitions of the topic
     * 3. preferred replica election in progress for some partitions of the topic
     *
     * @param topics Topics that should be marked ineligible for deletion. No op if the topic is was not previously queued up for deletion
     */
    public void markTopicIneligibleForDeletion(Set<String> topics) {
        if (isDeleteTopicEnabled) {
            Set<String> newTopicsToHaltDeletion = Sc.and(topicsToBeDeleted, topics);
            topicsIneligibleForDeletion.addAll(newTopicsToHaltDeletion);
            if (newTopicsToHaltDeletion.size() > 0)
                info(String.format("Halted deletion of topics %s", newTopicsToHaltDeletion));
        }
    }

    public Boolean isTopicIneligibleForDeletion(String topic) {
        if (isDeleteTopicEnabled) {
            return topicsIneligibleForDeletion.contains(topic);
        } else ;
        return true;
    }

    public Boolean isTopicDeletionInProgress(String topic) {
        if (isDeleteTopicEnabled) {
            return controller.replicaStateMachine.isAtLeastOneReplicaInDeletionStartedState(topic);
        } else
            return false;
    }

    public boolean isPartitionToBeDeleted(TopicAndPartition topicAndPartition) {
        if (isDeleteTopicEnabled) {
            return partitionsToBeDeleted.contains(topicAndPartition);
        } else
            return false;
    }

    public boolean isTopicQueuedUpForDeletion(String topic) {
        if (isDeleteTopicEnabled) {
            return topicsToBeDeleted.contains(topic);
        } else
            return false;
    }

    /**
     * Invoked by the delete-topic-thread to wait until events that either trigger, restart or halt topic deletion occur.
     * controllerLock should be acquired before invoking this API
     */
    private void awaitTopicDeletionNotification() {
        Utils.inLock(deleteLock, () -> {
            while (deleteTopicsThread.isRunning.get() && !deleteTopicStateChanged.compareAndSet(true, false)) {
                debug("Waiting for signal to start or continue topic deletion");
                try {
                    deleteTopicsCond.await();
                } catch (InterruptedException e) {
                    logger.error(e.getMessage(), e);
                    return;
                }
            }
        });
    }

    /**
     * Signals the delete-topic-thread to process topic deletion
     */
    private void resumeTopicDeletionThread() {
        deleteTopicStateChanged.set(true);
        Utils.inLock(deleteLock, () -> {
            deleteTopicsCond.signal();
        });
    }

    /**
     * Invoked by the StopReplicaResponse callback when it receives no error code for a replica of a topic to be deleted.
     * As part of this, the replicas are moved from ReplicaDeletionStarted to ReplicaDeletionSuccessful state. The delete
     * topic thread is notified so it can tear down the topic if all replicas of a topic have been successfully deleted
     *
     * @param replicas Replicas that were successfully deleted by the broker
     */
    private void completeReplicaDeletion(Set<PartitionAndReplica> replicas) {
        Set<PartitionAndReplica> successfullyDeletedReplicas = Sc.filter(replicas, r -> isTopicQueuedUpForDeletion(r.topic));
        debug(String.format("Deletion successfully completed for replicas %s", successfullyDeletedReplicas));
        controller.replicaStateMachine.handleStateChanges(successfullyDeletedReplicas, ReplicaDeletionSuccessful);
        resumeTopicDeletionThread();
    }

    /**
     * Topic deletion can be retried if -
     * 1. Topic deletion is not already complete
     * 2. Topic deletion is currently not in progress for that topic
     * 3. Topic is currently marked ineligible for deletion
     *
     * @param topic Topic
     * @return Whether or not deletion can be retried for the topic
     */
    private Boolean isTopicEligibleForDeletion(String topic) {
        return topicsToBeDeleted.contains(topic) && (!isTopicDeletionInProgress(topic) && !isTopicIneligibleForDeletion(topic));
    }

    /**
     * If the topic is queued for deletion but deletion is not currently under progress, then deletion is retried for that topic
     * To ensure a successful retry, reset states for respective replicas from ReplicaDeletionIneligible to OfflineReplica state
     *
     * @param topic Topic for which deletion should be retried
     */
    private void markTopicForDeletionRetry(String topic) {
        // reset replica states from ReplicaDeletionIneligible to OfflineReplica;
        Set<PartitionAndReplica> failedReplicas = controller.replicaStateMachine.replicasInState(topic, ReplicaDeletionIneligible);
        info(String.format("Retrying delete topic for topic %s since replicas %s were not successfully deleted", topic, failedReplicas));
        controller.replicaStateMachine.handleStateChanges(failedReplicas, OfflineReplica);
    }

    private void completeDeleteTopic(String topic) {
        // deregister partition change listener on the deleted topic. This is to prevent the partition change listener;
        // firing before the new topic listener when a deleted topic gets auto created;
        partitionStateMachine.deregisterPartitionChangeListener(topic);
        Set<PartitionAndReplica> replicasForDeletedTopic = controller.replicaStateMachine.replicasInState(topic, ReplicaDeletionSuccessful);
        // controller will remove this replica from the state machine as well as its partition assignment cache;
        replicaStateMachine.handleStateChanges(replicasForDeletedTopic, NonExistentReplica);
        Set<TopicAndPartition> partitionsForDeletedTopic = controllerContext.partitionsForTopic(topic);
        // move respective partition to OfflinePartition and NonExistentPartition state;
        partitionStateMachine.handleStateChanges(partitionsForDeletedTopic, OfflinePartition);
        partitionStateMachine.handleStateChanges(partitionsForDeletedTopic, NonExistentPartition);
        topicsToBeDeleted.remove(topic);
        partitionsToBeDeleted.removeIf(p -> p.topic == topic);
        controllerContext.zkClient.deleteRecursive(ZkUtils.getTopicPath(topic));
        controllerContext.zkClient.deleteRecursive(ZkUtils.getTopicConfigPath(topic));
        controllerContext.zkClient.delete(ZkUtils.getDeleteTopicPath(topic));
        controllerContext.removeTopic(topic);
    }

    /**
     * This callback is invoked by the DeleteTopics thread with the list of topics to be deleted
     * It invokes the delete partition callback for all partitions of a topic.
     * The updateMetadataRequest is also going to set the leader for the topics being deleted to
     * @link LeaderAndIsr#LeaderDuringDelete. This lets each broker know that this topic is being deleted and can be
     * removed from their caches.
     */
    private void onTopicDeletion(Set<String> topics) {
        info(String.format("Topic deletion callback for %s", topics));
        // send update metadata so that brokers stop serving data for topics to be deleted;
        Set<TopicAndPartition> partitions = Sc.flatMap(topics, t -> controllerContext.partitionsForTopic(t));
        controller.sendUpdateMetadataRequest(Sc.toList(controllerContext.liveOrShuttingDownBrokerIds()), partitions);
        Map<String, Map<TopicAndPartition, List<Integer>>> partitionReplicaAssignmentByTopic = Sc.groupByKey(controllerContext.partitionReplicaAssignment, k -> k.topic);
        topics.forEach(topic -> onPartitionDeletion(Sc.toSet(Sc.map(partitionReplicaAssignmentByTopic.get(topic), (k, v) -> k))));
    }

    /**
     * Invoked by the onPartitionDeletion callback. It is the 2nd step of topic deletion, the first being sending
     * UpdateMetadata requests to all brokers to start rejecting requests for deleted topics. As part of starting deletion,
     * the topics are added to the in progress list. As long as a topic is in the in progress list, deletion for that topic
     * is never retried. A topic is removed from the in progress list when
     * 1. Either the topic is successfully deleted OR
     * 2. No replica for the topic is in ReplicaDeletionStarted state and at least one replica is in ReplicaDeletionIneligible state
     * If the topic is queued for deletion but deletion is not currently under progress, then deletion is retried for that topic
     * As part of starting deletion, all replicas are moved to the ReplicaDeletionStarted state where the controller sends
     * the replicas a StopReplicaRequest (delete=true)
     * This callback does the following things -
     * 1. Move all dead replicas directly to ReplicaDeletionIneligible state. Also mark the respective topics ineligible
     * for deletion if some replicas are dead since it won't complete successfully anyway
     * 2. Move all alive replicas to ReplicaDeletionStarted state so they can be deleted successfully
     *
     * @param replicasForTopicsToBeDeleted
     */
    private void startReplicaDeletion(final Set<PartitionAndReplica> replicasForTopicsToBeDeleted) {
        Sc.groupBy(replicasForTopicsToBeDeleted, r -> r.topic).forEach((topic, replicas) -> {
            Set<PartitionAndReplica> aliveReplicasForTopic = Sc.filter(controllerContext.allLiveReplicas(), p -> p.topic.equals(topic));
            Set<PartitionAndReplica> deadReplicasForTopic = Sc.subtract(replicasForTopicsToBeDeleted, aliveReplicasForTopic);
            Set<PartitionAndReplica> successfullyDeletedReplicas = controller.replicaStateMachine.replicasInState(topic, ReplicaDeletionSuccessful);
            Set<PartitionAndReplica> replicasForDeletionRetry = Sc.subtract(aliveReplicasForTopic, successfullyDeletedReplicas);
            // move dead replicas directly to failed state;
            replicaStateMachine.handleStateChanges(deadReplicasForTopic, ReplicaDeletionIneligible);
            // send stop replica to all followers that are not in the OfflineReplica state so they stop sending fetch requests to the leader;
            replicaStateMachine.handleStateChanges(replicasForDeletionRetry, OfflineReplica);
            debug(String.format("Deletion started for replicas %s", replicasForDeletionRetry));
            controller.replicaStateMachine.handleStateChanges(replicasForDeletionRetry, ReplicaDeletionStarted,
                    new CallbackBuilder().stopReplicaCallback(deleteTopicStopReplicaCallback).build());
            if (deadReplicasForTopic.size() > 0) {
                debug(String.format("Dead Replicas (%s) found for topic %s", deadReplicasForTopic, topic));
                markTopicIneligibleForDeletion(Sets.newHashSet(topic));
            }
        });
    }

    /**
     * This callback is invoked by the delete topic callback with the list of partitions for topics to be deleted
     * It does the following -
     * 1. Send UpdateMetadataRequest to all live brokers (that are not shutting down) for partitions that are being
     * deleted. The brokers start rejecting all client requests with UnknownTopicOrPartitionException
     * 2. Move all replicas for the partitions to OfflineReplica state. This will send StopReplicaRequest to the replicas
     * and LeaderAndIsrRequest to the leader with the shrunk ISR. When the leader replica itself is moved to OfflineReplica state,
     * it will skip sending the LeaderAndIsrRequest since the leader will be updated to -1
     * 3. Move all replicas to ReplicaDeletionStarted state. This will send StopReplicaRequest with deletePartition=true. And
     * will delete all persistent data from all replicas of the respective partitions
     */
    private void onPartitionDeletion(Set<TopicAndPartition> partitionsToBeDeleted) {
        info(String.format("Partition deletion callback for %s", partitionsToBeDeleted));
        Set<PartitionAndReplica> replicasPerPartition = controllerContext.replicasForPartition(partitionsToBeDeleted);
        startReplicaDeletion(replicasPerPartition);
    }

    private ActionP2<RequestOrResponse, Integer> deleteTopicStopReplicaCallback = (stopReplicaResponseObj, replicaId) -> {
        StopReplicaResponse stopReplicaResponse = (StopReplicaResponse) stopReplicaResponseObj;
        debug(String.format("Delete topic callback invoked for %s", stopReplicaResponse));
        final Set<TopicAndPartition> partitionsInError;
        if (stopReplicaResponse.errorCode != ErrorMapping.NoError) {
            partitionsInError = stopReplicaResponse.responseMap.keySet();
        } else
            partitionsInError = Sc.toSet(Sc.map(Sc.filter(stopReplicaResponse.responseMap, (k, v) -> v != ErrorMapping.NoError), (k, v) -> k));
        Set<PartitionAndReplica> replicasInError = Sc.map(partitionsInError, p -> new PartitionAndReplica(p.topic, p.partition, replicaId));
        Utils.inLock(controllerContext.controllerLock, () -> {
            // move all the failed replicas to ReplicaDeletionIneligible;
            failReplicaDeletion(replicasInError);
            if (replicasInError.size() != stopReplicaResponse.responseMap.size()) {
                // some replicas could have been successfully deleted;
                Set<TopicAndPartition> deletedReplicas = Sc.subtract(stopReplicaResponse.responseMap.keySet(), partitionsInError);
                completeReplicaDeletion(Sc.map(deletedReplicas, p -> new PartitionAndReplica(p.topic, p.partition, replicaId)));
            }
        });
    };

    class DeleteTopicsThread extends ShutdownableThread {

        public DeleteTopicsThread() {
            super("delete-topics-thread-" + controller.config.brokerId, false);
        }

        public ZkClient zkClient = controllerContext.zkClient;

        @Override
        public void doWork() {
            awaitTopicDeletionNotification();

            if (!isRunning.get())
                return;

            Utils.inLock(controllerContext.controllerLock, () -> {
                Set<String> topicsQueuedForDeletion = Sc.add(topicsToBeDeleted);

                if (!topicsQueuedForDeletion.isEmpty())
                    info("Handling deletion for topics " + topicsQueuedForDeletion);

                topicsQueuedForDeletion.forEach(topic -> {
                    // if all replicas are marked as deleted successfully, then topic deletion is done;
                    if (controller.replicaStateMachine.areAllReplicasForTopicDeleted(topic)) {
                        // clear up all state for this topic from controller cache and zookeeper;
                        completeDeleteTopic(topic);
                        info(String.format("Deletion of topic %s successfully completed", topic));
                    } else {
                        if (controller.replicaStateMachine.isAtLeastOneReplicaInDeletionStartedState(topic)) {
                            // ignore since topic deletion is in progress;
                            Set<PartitionAndReplica> replicasInDeletionStartedState = controller.replicaStateMachine.replicasInState(topic, ReplicaDeletionStarted);
                            Set<Integer> replicaIds = Sc.map(replicasInDeletionStartedState, r -> r.replica);
                            Set<TopicAndPartition> partitions = Sc.map(replicasInDeletionStartedState, r -> new TopicAndPartition(r.topic, r.partition));
                            info(String.format("Deletion for replicas %s for partition %s of topic %s in progress", replicaIds, partitions, topic));
                        } else {
                            // if you come here, then no replica is in TopicDeletionStarted and all replicas are not in;
                            // TopicDeletionSuccessful. That means, that either given topic haven't initiated deletion;
                            // or there is at least one failed replica (which means topic deletion should be retried).;
                            if (controller.replicaStateMachine.isObjectReplicaInState(topic, ReplicaDeletionIneligible)) {
                                // mark topic for deletion retry;
                                markTopicForDeletionRetry(topic);
                            }
                        }
                    }
                    // Try delete topic if it is eligible for deletion.;
                    if (isTopicEligibleForDeletion(topic)) {
                        info(String.format("Deletion of topic %s (re)started", topic));
                        // topic deletion will be kicked off;
                        onTopicDeletion(Sets.newHashSet(topic));
                    } else if (isTopicIneligibleForDeletion(topic)) {
                        info(String.format("Not retrying deletion of topic %s at this time since it is marked ineligible for deletion", topic));
                    }
                });
            });
        }
    }
}
