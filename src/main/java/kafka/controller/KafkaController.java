package kafka.controller;

/**
 * @author zhoulf
 * @create 2017-11-02 19 14
 **/

import static kafka.server.BrokerStates.*;
import static kafka.controller.ReplicaState.*;
import static kafka.controller.partition.PartitionState.*;
import static kafka.utils.ZkUtils.getLeaderAndIsrForPartition;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.yammer.metrics.core.Gauge;
import kafka.admin.AdminUtils;
import kafka.admin.PreferredReplicaLeaderElectionCommand;
import kafka.api.LeaderAndIsr;
import kafka.api.RequestOrResponse;
import kafka.common.BrokerNotAvailableException;
import kafka.common.ControllerMovedException;
import kafka.common.KafkaException;
import kafka.common.StateChangeFailedException;
import kafka.controller.ctrl.ControllerContext;
import kafka.controller.ctrl.LeaderIsrAndControllerEpoch;
import kafka.controller.ctrl.PartitionAndReplica;
import kafka.func.*;
import kafka.log.LogConfig;
import kafka.log.TopicAndPartition;
import kafka.metrics.KafkaMetricsGroup;
import kafka.server.BrokerState;
import kafka.server.KafkaConfig;
import kafka.server.StateChangeLogger;
import kafka.server.ZookeeperLeaderElector;
import kafka.utils.*;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class KafkaController extends KafkaMetricsGroup {
    public static Logging logging = Logging.getLogger(KafkaController.class.getName());
    public static StateChangeLogger stateChangeLogger = new StateChangeLogger("state.change.logger");
    public static final Integer InitialControllerEpoch = 1;
    public static final int InitialControllerEpochZkVersion = 1;
    public KafkaConfig config;
    public ZkClient zkClient;
    public BrokerState brokerState;
    private boolean isRunning = true;
    public ControllerContext controllerContext;
    public PartitionStateMachine partitionStateMachine;
    public ReplicaStateMachine replicaStateMachine;
    private ZookeeperLeaderElector controllerElector;
    // have a separate scheduler for the controller to be able to start and stop independently of the;
// kafka server;
    private KafkaScheduler autoRebalanceScheduler = new KafkaScheduler(1);
    public TopicDeletionManager deleteTopicManager = null;
    public OfflinePartitionLeaderSelector offlinePartitionSelector;
    private ReassignedPartitionLeaderSelector reassignedPartitionLeaderSelector;
    private PreferredReplicaPartitionLeaderSelector preferredReplicaPartitionLeaderSelector;
    private ControlledShutdownLeaderSelector controlledShutdownPartitionLeaderSelector;
    private ControllerBrokerRequestBatch brokerRequestBatch;

    private PartitionsReassignedListener partitionReassignedListener;
    private PreferredReplicaElectionListener preferredReplicaElectionListener;

    public void init() {
        controllerContext = new ControllerContext(zkClient, config.zkSessionTimeoutMs);
        partitionStateMachine = new PartitionStateMachine(this);
        replicaStateMachine = new ReplicaStateMachine(this);
        controllerElector = new ZookeeperLeaderElector(controllerContext, ZkUtils.ControllerPath, onControllerFailover,
                onControllerResignation, config.brokerId);
        // have a separate scheduler for the controller to be able to start and stop independently of the;
// kafka server;
        offlinePartitionSelector = new OfflinePartitionLeaderSelector(controllerContext, config);
        reassignedPartitionLeaderSelector = new ReassignedPartitionLeaderSelector(controllerContext);
        preferredReplicaPartitionLeaderSelector = new PreferredReplicaPartitionLeaderSelector(controllerContext);
        controlledShutdownPartitionLeaderSelector = new ControlledShutdownLeaderSelector(controllerContext);
        brokerRequestBatch = new ControllerBrokerRequestBatch(this);

        partitionReassignedListener = new PartitionsReassignedListener(this);
        preferredReplicaElectionListener = new PreferredReplicaElectionListener(this);
    }

    public KafkaController(KafkaConfig config, ZkClient zkClient, BrokerState brokerState) throws StateChangeFailedException {
        this.config = config;
        this.zkClient = zkClient;
        this.brokerState = brokerState;
        stateChangeLogger = KafkaController.stateChangeLogger;
        newGauge("ActiveControllerCount", new Gauge<Object>() {
                    public Object value() {
                        if (isActive()) return 1;
                        else return 0;
                    }
                }
        );

        newGauge("OfflinePartitionsCount", new Gauge<Integer>() {
            public Integer value() {
                Utils.inLock(controllerContext.controllerLock, () -> {
                    if (!isActive())
                        return 0;
                    else
                        return Sc.count(controllerContext.partitionLeadershipInfo, (k, v) -> !controllerContext.liveOrShuttingDownBrokerIds().contains(v.leaderAndIsr.leader));
                });
                return null;
            }
        });

        newGauge("PreferredReplicaImbalanceCount", new Gauge<Integer>() {
                    public Integer value() {
                        return Utils.inLock(controllerContext.controllerLock, () -> {
                            if (!isActive())
                                return 0;
                            else
                                return Sc.count(controllerContext.partitionReplicaAssignment, (topicPartition, replicas) -> controllerContext.partitionLeadershipInfo.get(topicPartition).leaderAndIsr.leader != Sc.head(replicas));
                        });
                    }
                }
        );
        init();
    }


    public static Integer parseControllerId(String controllerInfoString) {
        try {
            Map<String, Object> controllerInfo = JSON.parseObject(controllerInfoString, Map.class);
            if (controllerInfo != null) {
                return Integer.parseInt(controllerInfo.get("brokerid").toString());
            } else {
                throw new KafkaException(String.format("Failed to parse the controller info json <%s>.", controllerInfoString));
            }
        } catch (Throwable t) {
            // It may be due to an incompatible controller register version;
            logging.warn(String.format("Failed to parse the controller info as json. " +
                    "Probably this controller is still using the old format <%s> to store the broker id in zookeeper", controllerInfoString));
            try {
                return Integer.parseInt(controllerInfoString);
            } catch (Throwable t2) {
                throw new KafkaException("Failed to parse the controller info: " + controllerInfoString + ". This is neither the new or the old format.", t2);
            }
        }
    }


    public Integer epoch() {
        return controllerContext.epoch;
    }

    public String clientId() {
        return String.format("id_%d-host_%s-port_%d", config.brokerId, config.hostName, config.port);
    }

    /**
     * On clean shutdown, the controller first determines the partitions that the
     * shutting down broker leads, and moves leadership of those partitions to another broker
     * that is in that partition's ISR.
     *
     * @param id Id of the broker to shutdown.
     * @return The number of partitions that the broker still leads.
     */
    public Set<TopicAndPartition> shutdownBroker(Integer id) {

        if (!isActive()) {
            throw new ControllerMovedException("Controller moved to another broker. Aborting controlled shutdown");
        }

        synchronized (controllerContext.brokerShutdownLock) {
            info("Shutting down broker " + id);

            Utils.inLock(controllerContext.controllerLock, () -> {
                if (!controllerContext.liveOrShuttingDownBrokerIds().contains(id))
                    throw new BrokerNotAvailableException(String.format("Broker id %d does not exist.", id));

                controllerContext.shuttingDownBrokerIds.add(id);
                debug("All shutting down brokers: " + controllerContext.shuttingDownBrokerIds);
                debug("Live brokers: " + controllerContext.liveBrokerIds());
            });

            Set<Tuple<TopicAndPartition, Integer>> allPartitionsAndReplicationFactorOnBroker =
                    Utils.inLock(controllerContext.controllerLock, () ->
                            Sc.map(controllerContext.partitionsOnBroker(id), topicAndPartition ->
                                    Tuple.of(topicAndPartition, controllerContext.partitionReplicaAssignment.get(topicAndPartition).size()))
                    );

            Sc.foreach(allPartitionsAndReplicationFactorOnBroker, (topicAndPartition, replicationFactor) -> {
                // Move leadership serially to relinquish lock.;
                Utils.inLock(controllerContext.controllerLock, () -> {
                    kafka.controller.ctrl.LeaderIsrAndControllerEpoch currLeaderIsrAndControllerEpoch = controllerContext.partitionLeadershipInfo.get(topicAndPartition);
                    if (currLeaderIsrAndControllerEpoch != null) {
                        if (replicationFactor > 1) {
                            if (currLeaderIsrAndControllerEpoch.leaderAndIsr.leader == id) {
                                // If the broker leads the topic partition, transition the leader and update isr. Updates zk and;
                                // notifies all affected brokers;
                                partitionStateMachine.handleStateChanges(Sets.newHashSet(topicAndPartition), OnlinePartition, controlledShutdownPartitionLeaderSelector, null);
                            } else {
                                // Stop the replica first. The state change below initiates ZK changes which should take some time;
                                // before which the stop replica request should be completed (in most cases)
                                brokerRequestBatch.newBatch();
                                brokerRequestBatch.addStopReplicaRequestForBrokers(Lists.newArrayList(id), topicAndPartition.topic, topicAndPartition.partition, false, null);
                                brokerRequestBatch.sendRequestsToBrokers(epoch(), controllerContext.correlationId.getAndIncrement());

                                // If the broker is a follower, updates the isr in ZK and notifies the current leader;
                                replicaStateMachine.handleStateChanges(Sets.newHashSet(new PartitionAndReplica(topicAndPartition.topic, topicAndPartition.partition, id)), OfflineReplica);
                            }
                        }
                    }
                });
            });
        }
        return Sc.toSet(replicatedPartitionsBrokerLeads(id));
    }

    public List<TopicAndPartition> replicatedPartitionsBrokerLeads(Integer brokerId) {
        return Utils.inLock(controllerContext.controllerLock, () -> {
            trace("All leaders = " + controllerContext.partitionLeadershipInfo);
            return Sc.map(Sc.filter(controllerContext.partitionLeadershipInfo, (topicAndPartition, leaderIsrAndControllerEpoch) ->
                    leaderIsrAndControllerEpoch.leaderAndIsr.leader == brokerId && controllerContext.partitionReplicaAssignment.get(topicAndPartition).size() > 1), e -> e.getKey());
        });
    }

    /**
     * This callback is invoked by the zookeeper leader elector on electing the current broker as the new controller.
     * It does the following things on the become-controller state change -
     * 1. Register controller epoch changed listener
     * 2. Increments the controller epoch
     * 3. Initializes the controller's context object that holds cache objects for current topics, live brokers and
     * leaders for all existing partitions.
     * 4. Starts the controller's channel manager
     * 5. Starts the replica state machine
     * 6. Starts the partition state machine
     * If it encounters any unexpected exception/error while becoming controller, it resigns as the current controller.
     * This ensures another controller election will be triggered and there will always be an actively serving controller
     */
    public ActionWithThrow onControllerFailover = () -> {
        if (isRunning) {
            info(String.format("Broker %d starting become controller state transition", config.brokerId));
            //read controller epoch from zk;
            readControllerEpochFromZookeeper();
            // increment the controller epoch;
            incrementControllerEpoch(zkClient);
            // before reading source of truth from zookeeper, register the listeners to get broker/topic callbacks;
            registerReassignedPartitionsListener();
            registerPreferredReplicaElectionListener();
            partitionStateMachine.registerListeners();
            replicaStateMachine.registerListeners();
            initializeControllerContext();
            replicaStateMachine.startup();
            partitionStateMachine.startup();
            // register the partition change listeners for all existing topics on failover;
            controllerContext.allTopics.forEach(topic -> partitionStateMachine.registerPartitionChangeListener(topic));
            info(String.format("Broker %d is ready to serve as the new controller with epoch %d", config.brokerId, epoch()));
            brokerState.newState(RunningAsController);
            maybeTriggerPartitionReassignment();
            maybeTriggerPreferredReplicaElection();
      /* send partition leadership info to all live brokers */
            sendUpdateMetadataRequest(Sc.toList(controllerContext.liveOrShuttingDownBrokerIds()));
            if (config.autoLeaderRebalanceEnable) {
                info("starting the partition rebalance scheduler");
                autoRebalanceScheduler.startup();
                autoRebalanceScheduler.schedule("partition-rebalance-thread", () -> {
                            if (isActive()) {
                                trace("checking need to trigger partition rebalance");
                                // get all the active brokers;
                                Map<Integer, Map<TopicAndPartition, List<Integer>>> preferredReplicasForTopicsByBrokers = Utils.inLock(controllerContext.controllerLock, () -> {
                                    Map<TopicAndPartition, List<Integer>> t = Sc.filterNot(controllerContext.partitionReplicaAssignment,
                                            (k, v) -> deleteTopicManager.isTopicQueuedUpForDeletion(k.topic));
                                    return Sc.groupByValue(t, assignedReplicas -> Sc.head(assignedReplicas));
                                });
                                debug("preferred replicas by broker " + preferredReplicasForTopicsByBrokers);
                                // for each broker, check if a preferred replica election needs to be triggered;
                                preferredReplicasForTopicsByBrokers.forEach((leaderBroker, topicAndPartitionsForBroker) -> {
                                    NumCount<Double> imbalanceRatio = NumCount.of(0D);
                                    Map<TopicAndPartition, List<Integer>> topicsNotInPreferredReplica = Utils.inLock(controllerContext.controllerLock, () -> {
                                        Map<TopicAndPartition, List<Integer>> topicsNotInPreferredReplica2 =
                                                Sc.filter(topicAndPartitionsForBroker, (topicPartition, replicas) ->
                                                        controllerContext.partitionLeadershipInfo.containsKey(topicPartition) &&
                                                                controllerContext.partitionLeadershipInfo.get(topicPartition).leaderAndIsr.leader != leaderBroker);
                                        debug("topics not in preferred replica " + topicsNotInPreferredReplica2);
                                        int totalTopicPartitionsForBroker = topicAndPartitionsForBroker.size();
                                        int totalTopicPartitionsNotLedByBroker = topicsNotInPreferredReplica2.size();
                                        imbalanceRatio.set((double) totalTopicPartitionsNotLedByBroker / totalTopicPartitionsForBroker);
                                        trace(String.format("leader imbalance ratio for broker %d is %f", leaderBroker, imbalanceRatio.get()));
                                        return topicsNotInPreferredReplica2;
                                    });
                                    // check ratio and if greater than desired ratio, trigger a rebalance for the topic partitions;
                                    // that need to be on this broker;
                                    if (imbalanceRatio.get() > ((double) config.leaderImbalancePerBrokerPercentage / 100)) {
                                        topicsNotInPreferredReplica.forEach((topicPartition, replicas) -> {
                                            Utils.inLock(controllerContext.controllerLock, () -> {
                                                // do this check only if the broker is live and there are no partitions being reassigned currently;
                                                // and preferred replica election is not in progress;
                                                if (controllerContext.liveBrokerIds().contains(leaderBroker) &&
                                                        controllerContext.partitionsBeingReassigned.size() == 0 &&
                                                        controllerContext.partitionsUndergoingPreferredReplicaElection.size() == 0 &&
                                                        !deleteTopicManager.isTopicQueuedUpForDeletion(topicPartition.topic) &&
                                                        controllerContext.allTopics.contains(topicPartition.topic)) {
                                                    onPreferredReplicaElection(Sets.newHashSet(topicPartition), true);
                                                }
                                            });
                                        });
                                    }
                                });
                            }
                        },
                        5L, config.leaderImbalanceCheckIntervalSeconds.longValue(), TimeUnit.SECONDS);
            }
            deleteTopicManager.start();
        } else
            info("Controller has been shut down, aborting startup/failover");
    };

    /**
     * This callback is invoked by the zookeeper leader elector when the current broker resigns as the controller. This is
     * required to clean up internal controller data structures
     */
    public Action onControllerResignation = () -> {
        // de-register listeners;
        deregisterReassignedPartitionsListener();
        deregisterPreferredReplicaElectionListener();

        // shutdown delete topic manager;
        if (deleteTopicManager != null)
            deleteTopicManager.shutdown();

        // shutdown leader rebalance scheduler;
        if (config.autoLeaderRebalanceEnable)
            autoRebalanceScheduler.shutdown();

        Utils.inLock(controllerContext.controllerLock, () -> {
            // de-register partition ISR listener for on-going partition reassignment task;
            deregisterReassignedPartitionsIsrChangeListeners();
            // shutdown partition state machine;
            partitionStateMachine.shutdown();
            // shutdown replica state machine;
            replicaStateMachine.shutdown();
            // shutdown controller channel manager;
            if (controllerContext.controllerChannelManager != null) {
                controllerContext.controllerChannelManager.shutdown();
                controllerContext.controllerChannelManager = null;
            }
            // reset controller context;
            controllerContext.epoch = 0;
            controllerContext.epochZkVersion = 0;
            brokerState.newState(RunningAsBroker);
        });
    };

    /**
     * Returns true if this broker is the current controller.
     */
    public Boolean isActive() {
        return Utils.inLock(controllerContext.controllerLock, () -> controllerContext.controllerChannelManager != null);
    }

    /**
     * This callback is invoked by the replica state machine's broker change listener, with the list of newly started
     * brokers as input. It does the following -
     * 1. Triggers the OnlinePartition state change for all new/offline partitions
     * 2. It checks whether there are reassigned replicas assigned to any newly started brokers.  If
     * so, it performs the reassignment logic for each topic/partition.
     * <p>
     * Note that we don't need to refresh the leader/isr cache for all topic/partitions at this point for two reasons:
     * 1. The partition state machine, when triggering online state change, will refresh leader and ISR for only those
     * partitions currently new or offline (rather than every partition this controller is aware of)
     * 2. Even if we do refresh the cache, there is no guarantee that by the time the leader and ISR request reaches
     * every broker that it is still valid.  Brokers check the leader epoch to determine validity of the request.
     */
    public void onBrokerStartup(List<Integer> newBrokers) {
        info(String.format("New broker startup callback for %s", newBrokers));
        Set<Integer> newBrokersSet = Sc.toSet(newBrokers);
        // send update metadata request for all partitions to the newly restarted brokers. In cases of controlled shutdown;
        // leaders will not be elected when a new broker comes up. So at least in the common controlled shutdown case, the;
        // metadata will reach the new brokers faster;
        sendUpdateMetadataRequest(newBrokers);
        // the very first thing to do when a new broker comes up is send it the entire list of partitions that it is;
        // supposed to host. Based on that the broker starts the high watermark threads for the input list of partitions;
        Set<PartitionAndReplica> allReplicasOnNewBrokers = controllerContext.replicasOnBrokers(newBrokersSet);
        replicaStateMachine.handleStateChanges(allReplicasOnNewBrokers, OnlineReplica);
        // when a new broker comes up, the controller needs to trigger leader election for all new and offline partitions;
        // to see if these brokers can become leaders for some/all of those;
        partitionStateMachine.triggerOnlinePartitionStateChange();
        // check if reassignment of some partitions need to be restarted;
        Map<TopicAndPartition, ReassignedPartitionsContext> partitionsWithReplicasOnNewBrokers = Sc.filter(controllerContext.partitionsBeingReassigned, (topicAndPartition, reassignmentContext) -> Sc.exists(reassignmentContext.newReplicas, r -> newBrokersSet.contains(r)));
        partitionsWithReplicasOnNewBrokers.forEach((topicAndPartition, reassignedPartitionsContext) -> onPartitionReassignment(topicAndPartition, reassignedPartitionsContext));
        // check if topic deletion needs to be resumed. If at least one replica that belongs to the topic being deleted exists;
        // on the newly restarted brokers, there is a chance that topic deletion can resume;
        Set<PartitionAndReplica> replicasForTopicsToBeDeleted = Sc.filter(allReplicasOnNewBrokers, p -> deleteTopicManager.isTopicQueuedUpForDeletion(p.topic));
        if (replicasForTopicsToBeDeleted.size() > 0) {
            info(String.format("Some replicas %s for topics scheduled for deletion %s are on the newly restarted brokers %s. " +
                            "Signaling restart of topic deletion for these topics", replicasForTopicsToBeDeleted,
                    deleteTopicManager.topicsToBeDeleted, newBrokers));
            deleteTopicManager.resumeDeletionForTopics(Sc.map(replicasForTopicsToBeDeleted, r -> r.topic));
        }
    }

    /**
     * This callback is invoked by the replica state machine's broker change listener with the list of failed brokers
     * as input. It does the following -
     * 1. Mark partitions with dead leaders as offline
     * 2. Triggers the OnlinePartition state change for all new/offline partitions
     * 3. Invokes the OfflineReplica state change on the input list of newly started brokers
     * <p>
     * Note that we don't need to refresh the leader/isr cache for all topic/partitions at this point.  This is because
     * the partition state machine will refresh our cache for us when performing leader election for all new/offline
     * partitions coming online.
     */
    public void onBrokerFailure(List<Integer> deadBrokers) {
        info(String.format("Broker failure callback for %s", deadBrokers));
        List<Integer> deadBrokersThatWereShuttingDown = Sc.filter(deadBrokers, id -> controllerContext.shuttingDownBrokerIds.remove(id));
        info(String.format("Removed %s from list of shutting down brokers.", deadBrokersThatWereShuttingDown));
        Set<Integer> deadBrokersSet = Sc.toSet(deadBrokers);
        // trigger OfflinePartition state for all partitions whose current leader is one amongst the dead brokers;
        Set<TopicAndPartition> partitionsWithoutLeader = Sc.filter(controllerContext.partitionLeadershipInfo, (k, v) ->
                deadBrokersSet.contains(v.leaderAndIsr.leader) &&
                        !deleteTopicManager.isTopicQueuedUpForDeletion(k.topic)).keySet();
        partitionStateMachine.handleStateChanges(partitionsWithoutLeader, OfflinePartition);
        // trigger OnlinePartition state changes for offline or new partitions;
        partitionStateMachine.triggerOnlinePartitionStateChange();
        // filter out the replicas that belong to topics that are being deleted;
        Set<PartitionAndReplica> allReplicasOnDeadBrokers = controllerContext.replicasOnBrokers(deadBrokersSet);
        Set<PartitionAndReplica> activeReplicasOnDeadBrokers = Sc.filterNot(allReplicasOnDeadBrokers, p -> deleteTopicManager.isTopicQueuedUpForDeletion(p.topic));
        // handle dead replicas;
        replicaStateMachine.handleStateChanges(activeReplicasOnDeadBrokers, OfflineReplica);
        // check if topic deletion state for the dead replicas needs to be updated;
        Set<PartitionAndReplica> replicasForTopicsToBeDeleted = Sc.filter(allReplicasOnDeadBrokers, p -> deleteTopicManager.isTopicQueuedUpForDeletion(p.topic));
        if (replicasForTopicsToBeDeleted.size() > 0) {
            // it is required to mark the respective replicas in TopicDeletionFailed state since the replica cannot be;
            // deleted when the broker is down. This will prevent the replica from being in TopicDeletionStarted state indefinitely;
            // since topic deletion cannot be retried until at least one replica is in TopicDeletionStarted state;
            deleteTopicManager.failReplicaDeletion(replicasForTopicsToBeDeleted);
        }
    }

    /**
     * This callback is invoked by the partition state machine's topic change listener with the list of new topics
     * and partitions as input. It does the following -
     * 1. Registers partition change listener. This is not required until KAFKA-347
     * 2. Invokes the new partition callback
     * 3. Send metadata request with the new topic to all brokers so they allow requests for that topic to be served
     */
    public void onNewTopicCreation(Set<String> topics, Set<TopicAndPartition> newPartitions) {
        info(String.format("New topic creation callback for %s", newPartitions));
        // subscribe to partition changes;
        topics.forEach(topic -> partitionStateMachine.registerPartitionChangeListener(topic));
        onNewPartitionCreation(newPartitions);
    }

    /**
     * This callback is invoked by the topic change callback with the list of failed brokers as input.
     * It does the following -
     * 1. Move the newly created partitions to the NewPartition state
     * 2. Move the newly created partitions from NewPartition->OnlinePartition state
     */
    public void onNewPartitionCreation(Set<TopicAndPartition> newPartitions) {
        info(String.format("New partition creation callback for %s", newPartitions));
        partitionStateMachine.handleStateChanges(newPartitions, NewPartition);
        replicaStateMachine.handleStateChanges(controllerContext.replicasForPartition(newPartitions), NewReplica);
        partitionStateMachine.handleStateChanges(newPartitions, OnlinePartition, offlinePartitionSelector, null);
        replicaStateMachine.handleStateChanges(controllerContext.replicasForPartition(newPartitions), OnlineReplica);
    }

    /**
     * This callback is invoked by the reassigned partitions listener. When an admin command initiates a partition
     * reassignment, it creates the /admin/reassign_partitions path that triggers the zookeeper listener.
     * Reassigning replicas for a partition goes through a few steps listed in the code.
     * RAR = Reassigned replicas
     * OAR = Original list of replicas for partition
     * AR = current assigned replicas
     * <p>
     * 1. Update AR in ZK with OAR + RAR.
     * 2. Send LeaderAndIsr request to every replica in OAR + RAR (with AR as OAR + RAR). We do this by forcing an update
     * of the leader epoch in zookeeper.
     * 3. Start new replicas RAR - OAR by moving replicas in RAR - OAR to NewReplica state.
     * 4. Wait until all replicas in RAR are in sync with the leader.
     * 5  Move all replicas in RAR to OnlineReplica state.
     * 6. Set AR to RAR in memory.
     * 7. If the leader is not in RAR, elect a new leader from RAR. If new leader needs to be elected from RAR, a LeaderAndIsr
     * will be sent. If not, then leader epoch will be incremented in zookeeper and a LeaderAndIsr request will be sent.
     * In any case, the LeaderAndIsr request will have AR = RAR. This will prevent the leader from adding any replica in
     * RAR - OAR back in the isr.
     * 8. Move all replicas in OAR - RAR to OfflineReplica state. As part of OfflineReplica state change, we shrink the
     * isr to remove OAR - RAR in zookeeper and sent a LeaderAndIsr ONLY to the Leader to notify it of the shrunk isr.
     * After that, we send a StopReplica (delete = false) to the replicas in OAR - RAR.
     * 9. Move all replicas in OAR - RAR to NonExistentReplica state. This will send a StopReplica (delete = false) to
     * the replicas in OAR - RAR to physically delete the replicas on disk.
     * 10. Update AR in ZK with RAR.
     * 11. Update the /admin/reassign_partitions path in ZK to remove this partition.
     * 12. After electing leader, the replicas and isr information changes. So resend the update metadata request to every broker.
     * <p>
     * For example, if OAR = {1, 2, 3} and RAR = {4,5,6}, the values in the assigned replica (AR) and leader/isr path in ZK
     * may go through the following transition.
     * AR                 leader/isr
     * {1,2,3}            1/{1,2,3}           (initial state)
     * {1,2,3,4,5,6}      1/{1,2,3}           (step 2)
     * {1,2,3,4,5,6}      1/{1,2,3,4,5,6}     (step 4)
     * {1,2,3,4,5,6}      4/{1,2,3,4,5,6}     (step 7)
     * {1,2,3,4,5,6}      4/{4,5,6}           (step 8)
     * {4,5,6}            4/{4,5,6}           (step 10)
     * <p>
     * Note that we have to update AR in ZK with RAR last since it's the only place where we store OAR persistently.
     * This way, if the controller crashes before that step, we can still recover.
     */
    public void onPartitionReassignment(TopicAndPartition topicAndPartition, ReassignedPartitionsContext reassignedPartitionContext) {
        List<Integer> reassignedReplicas = reassignedPartitionContext.newReplicas;
        if (!areReplicasInIsr(topicAndPartition.topic, topicAndPartition.partition, reassignedReplicas)) {
            info(String.format("New replicas %s for partition %s being ", reassignedReplicas, topicAndPartition) +
                    "reassigned not yet caught up with the leader");
            Set<Integer> newReplicasNotInOldReplicaList = Sc.subtract(Sc.toSet(reassignedReplicas), Sc.toSet(controllerContext.partitionReplicaAssignment.get(topicAndPartition)));

            Set<Integer> newAndOldReplicas = Sc.add(Sc.toSet(reassignedPartitionContext.newReplicas), controllerContext.partitionReplicaAssignment.get(topicAndPartition));
            //1. Update AR in ZK with OAR + RAR.;
            updateAssignedReplicasForPartition(topicAndPartition, Sc.toList(newAndOldReplicas));
            //2. Send LeaderAndIsr request to every replica in OAR + RAR (with AR as OAR + RAR).;
            updateLeaderEpochAndSendRequest(topicAndPartition, controllerContext.partitionReplicaAssignment.get(topicAndPartition), Sc.toList(newAndOldReplicas));
            //3. replicas in RAR - OAR -> NewReplica;
            startNewReplicasForReassignedPartition(topicAndPartition, reassignedPartitionContext, newReplicasNotInOldReplicaList);
            info(String.format("Waiting for new replicas %s for partition %s being ", reassignedReplicas, topicAndPartition) +
                    "reassigned to catch up with the leader");
        } else {
            //4. Wait until all replicas in RAR are in sync with the leader.;
            Set<Integer> oldReplicas = Sc.subtract(Sc.toSet(controllerContext.partitionReplicaAssignment.get(topicAndPartition)), Sc.toSet(reassignedReplicas));
            //5. replicas in RAR -> OnlineReplica;
            reassignedReplicas.forEach(replica ->
                    replicaStateMachine.handleStateChanges(Sets.newHashSet(new PartitionAndReplica(topicAndPartition.topic, topicAndPartition.partition, replica)), OnlineReplica)
            );
            //6. Set AR to RAR in memory.;
            //7. Send LeaderAndIsr request with a potential new leader (if current leader not in RAR) and;
            //   a new AR (using RAR) and same isr to every broker in RAR;
            moveReassignedPartitionLeaderIfRequired(topicAndPartition, reassignedPartitionContext);
            //8. replicas in OAR - RAR -> Offline (force those replicas out of isr)
            //9. replicas in OAR - RAR -> NonExistentReplica (force those replicas to be deleted)
            stopOldReplicasOfReassignedPartition(topicAndPartition, reassignedPartitionContext, oldReplicas);
            //10. Update AR in ZK with RAR.;
            updateAssignedReplicasForPartition(topicAndPartition, reassignedReplicas);
            //11. Update the /admin/reassign_partitions path in ZK to remove this partition.;
            removePartitionFromReassignedPartitions(topicAndPartition);
            info(String.format("Removed partition %s from the list of reassigned partitions in zookeeper", topicAndPartition));
            controllerContext.partitionsBeingReassigned.remove(topicAndPartition);
            //12. After electing leader, the replicas and isr information changes, so resend the update metadata request to every broker;
            sendUpdateMetadataRequest(Sc.toList(controllerContext.liveOrShuttingDownBrokerIds()), Sets.newHashSet(topicAndPartition));
            // signal delete topic thread if reassignment for some partitions belonging to topics being deleted just completed;
            deleteTopicManager.resumeDeletionForTopics(Sets.newHashSet(topicAndPartition.topic));
        }
    }

    private void watchIsrChangesForReassignedPartition(String topic, Integer partition, ReassignedPartitionsContext
            reassignedPartitionContext) {
        List<Integer> reassignedReplicas = reassignedPartitionContext.newReplicas;
        ReassignedPartitionsIsrChangeListener isrChangeListener = new ReassignedPartitionsIsrChangeListener(this, topic, partition, Sc.toSet(reassignedReplicas));
        reassignedPartitionContext.isrChangeListener = isrChangeListener;
        // register listener on the leader and isr path to wait until they catch up with the current leader;
        zkClient.subscribeDataChanges(ZkUtils.getTopicPartitionLeaderAndIsrPath(topic, partition), isrChangeListener);
    }

    public void initiateReassignReplicasForTopicPartition(TopicAndPartition topicAndPartition, ReassignedPartitionsContext reassignedPartitionContext) {
        List<Integer> newReplicas = reassignedPartitionContext.newReplicas;
        String topic = topicAndPartition.topic;
        Integer partition = topicAndPartition.partition;
        List<Integer> aliveNewReplicas = Sc.filter(newReplicas, r -> controllerContext.liveBrokerIds().contains(r));
        try {
            List<Integer> assignedReplicas = controllerContext.partitionReplicaAssignment.get(topicAndPartition);
            if (assignedReplicas != null) {
                // TODO: 2017/11/10
                if (assignedReplicas == newReplicas) {
                    throw new KafkaException(String.format("Partition %s to be reassigned is already assigned to replicas", topicAndPartition) +
                            String.format(" %s. Ignoring request for partition reassignment", newReplicas));
                } else {
                    if (Sc.equals(aliveNewReplicas, newReplicas)) {
                        info(String.format("Handling reassignment of partition %s to new replicas %s", topicAndPartition, newReplicas));
                        // first register ISR change listener;
                        watchIsrChangesForReassignedPartition(topic, partition, reassignedPartitionContext);
                        controllerContext.partitionsBeingReassigned.put(topicAndPartition, reassignedPartitionContext);
                        // mark topic ineligible for deletion for the partitions being reassigned;
                        deleteTopicManager.markTopicIneligibleForDeletion(Sets.newHashSet(topic));
                        onPartitionReassignment(topicAndPartition, reassignedPartitionContext);
                    } else {
                        // some replica in RAR is not alive. Fail partition reassignment;
                        throw new KafkaException(String.format("Only %s replicas out of the new set of replicas", aliveNewReplicas) +
                                String.format(" %s for partition %s to be reassigned are alive. ", newReplicas, topicAndPartition) +
                                "Failing partition reassignment");
                    }
                }
            } else {
                throw new KafkaException(String.format("Attempt to reassign partition %s that doesn't exist", topicAndPartition));
            }
        } catch (Throwable e) {
            error(String.format("Error completing reassignment of partition %s", topicAndPartition), e);
            // remove the partition from the admin path to unblock the admin client;
            removePartitionFromReassignedPartitions(topicAndPartition);
        }
    }

    public void onPreferredReplicaElection(Set<TopicAndPartition> partitions) {
        onPreferredReplicaElection(partitions, false);
    }

    public void onPreferredReplicaElection(Set<TopicAndPartition> partitions, Boolean isTriggeredByAutoRebalance) {
        info(String.format("Starting preferred replica leader election for partitions %s", partitions));
        try {
            controllerContext.partitionsUndergoingPreferredReplicaElection.addAll(partitions);
            deleteTopicManager.markTopicIneligibleForDeletion(Sc.map(partitions, p -> p.topic));
            partitionStateMachine.handleStateChanges(partitions, OnlinePartition, preferredReplicaPartitionLeaderSelector, null);
        } catch (Throwable e) {
            error(String.format("Error completing preferred replica leader election for partitions %s", partitions), e);
        } finally {
            removePartitionsFromPreferredReplicaElection(partitions, isTriggeredByAutoRebalance);
            deleteTopicManager.resumeDeletionForTopics(Sc.map(partitions, p -> p.topic));
        }
    }

    /**
     * Invoked when the controller module of a Kafka server is started up. This does not assume that the current broker
     * is the controller. It merely registers the session expiration listener and starts the controller leader
     * elector
     */
    public void startup() {
        Utils.inLock(controllerContext.controllerLock, () -> {
            info("Controller starting up");
            registerSessionExpirationListener();
            isRunning = true;
            controllerElector.startup();
            info("Controller startup complete");
        });
    }

    /**
     * Invoked when the controller module of a Kafka server is shutting down. If the broker was the current controller,
     * it shuts down the partition and replica state machines. If not, those are a no-op. In addition to that, it also
     * shuts down the controller channel manager, if one exists (i.e. if it was the current controller)
     */
    public void shutdown() throws Throwable {
        Utils.inLock(controllerContext.controllerLock, () -> isRunning = false);
        onControllerResignation.invoke();
    }

    public void sendRequest(Integer brokerId, RequestOrResponse request, ActionP<RequestOrResponse> callback) {
        controllerContext.controllerChannelManager.sendRequest(brokerId, request, callback);
    }

    public void incrementControllerEpoch(ZkClient zkClient) {
        try {
            Integer newControllerEpoch = controllerContext.epoch + 1;
            Tuple<Boolean, Integer> t = ZkUtils.conditionalUpdatePersistentPathIfExists(zkClient,
                    ZkUtils.ControllerEpochPath, newControllerEpoch.toString(), controllerContext.epochZkVersion);
            boolean updateSucceeded = t.v1;
            int newVersion = t.v2;
            if (!updateSucceeded)
                throw new ControllerMovedException("Controller moved to another broker. Aborting controller startup procedure");
            else {
                controllerContext.epochZkVersion = newVersion;
                controllerContext.epoch = newControllerEpoch;
            }
        } catch (ZkNoNodeException nne) {
            // if path doesn't exist, this is the first controller whose epoch should be 1;
            // the following call can still fail if another controller gets elected between checking if the path exists and;
            // trying to create the controller epoch path;
            try {
                zkClient.createPersistent(ZkUtils.ControllerEpochPath, KafkaController.InitialControllerEpoch.toString());
                controllerContext.epoch = KafkaController.InitialControllerEpoch;
                controllerContext.epochZkVersion = KafkaController.InitialControllerEpochZkVersion;
            } catch (ZkNodeExistsException e) {
                throw new ControllerMovedException("Controller moved to another broker. " +
                        "Aborting controller startup procedure");
            } catch (Throwable oe) {
                error("Error while incrementing controller epoch", oe);
            }
        } catch (Throwable oe) {
            error("Error while incrementing controller epoch", oe);
        }

        info(String.format("Controller %d incremented epoch to %d", config.brokerId, controllerContext.epoch));
    }

    private void registerSessionExpirationListener() {
        zkClient.subscribeStateChanges(new SessionExpirationListener());
    }

    private void initializeControllerContext() throws Throwable {
        // update controller cache with delete topic information;
        controllerContext.liveBrokers_(Sc.toSet(ZkUtils.getAllBrokersInCluster(zkClient)));
        controllerContext.allTopics = Sc.toSet(ZkUtils.getAllTopics(zkClient));
        controllerContext.partitionReplicaAssignment = ZkUtils.getReplicaAssignmentForTopics(zkClient, Sc.toList(controllerContext.allTopics));
        controllerContext.partitionLeadershipInfo = Maps.newHashMap();
        controllerContext.shuttingDownBrokerIds = Sets.newHashSet();
// update the leader and isr cache for all existing partitions from Zookeeper;
        updateLeaderAndIsrCache();
        // start the channel manager;
        startChannelManager();
        initializePreferredReplicaElection();
        initializePartitionReassignment();
        initializeTopicDeletion();
        info(String.format("Currently active brokers in the cluster: %s", controllerContext.liveBrokerIds()));
        info(String.format("Currently shutting brokers in the cluster: %s", controllerContext.shuttingDownBrokerIds));
        info(String.format("Current list of topics in the cluster: %s", controllerContext.allTopics));
    }

    private void initializePreferredReplicaElection() {
        // initialize preferred replica election state;
        Set<TopicAndPartition> partitionsUndergoingPreferredReplicaElection = ZkUtils.getPartitionsUndergoingPreferredReplicaElection(zkClient);
        // check if they are already completed or topic was deleted;
        Set<TopicAndPartition> partitionsThatCompletedPreferredReplicaElection = Sc.filter(partitionsUndergoingPreferredReplicaElection, partition -> {
            List<Integer> replicasOpt = controllerContext.partitionReplicaAssignment.get(partition);
            boolean topicDeleted = replicasOpt.isEmpty();
            boolean successful;
            if (!topicDeleted)
                successful = controllerContext.partitionLeadershipInfo.get(partition).leaderAndIsr.leader == Sc.head(replicasOpt);
            else successful = false;
            return successful || topicDeleted;
        });
        controllerContext.partitionsUndergoingPreferredReplicaElection.addAll(partitionsUndergoingPreferredReplicaElection);
        controllerContext.partitionsUndergoingPreferredReplicaElection.removeAll(partitionsThatCompletedPreferredReplicaElection);
        info(String.format("Partitions undergoing preferred replica election: %s", partitionsUndergoingPreferredReplicaElection));
        info(String.format("Partitions that completed preferred replica election: %s", partitionsThatCompletedPreferredReplicaElection));
        info(String.format("Resuming preferred replica election for partitions: %s", controllerContext.partitionsUndergoingPreferredReplicaElection));
    }

    private void initializePartitionReassignment() {
        // read the partitions being reassigned from zookeeper path /admin/reassign_partitions;
        Map<TopicAndPartition, ReassignedPartitionsContext> partitionsBeingReassigned = ZkUtils.getPartitionsBeingReassigned(zkClient);
        // check if they are already completed or topic was deleted;
        List<TopicAndPartition> reassignedPartitions = Sc.map(Sc.filter(partitionsBeingReassigned, (tp, context) -> {
            List<Integer> replicasOpt = controllerContext.partitionReplicaAssignment.get(tp);
            boolean topicDeleted = replicasOpt.isEmpty();
            boolean successful = (!topicDeleted) ? Sc.equals(replicasOpt, context.newReplicas) : false;
            return topicDeleted || successful;
        }), (k, v) -> k);
        reassignedPartitions.forEach(p -> removePartitionFromReassignedPartitions(p));
        Map<TopicAndPartition, ReassignedPartitionsContext> partitionsToReassign = Maps.newHashMap();
        partitionsToReassign.putAll(partitionsBeingReassigned);
        reassignedPartitions.forEach(p -> partitionsToReassign.remove(p));
        controllerContext.partitionsBeingReassigned.putAll(partitionsToReassign);
        info(String.format("Partitions being reassigned: %s", partitionsBeingReassigned.toString()));
        info(String.format("Partitions already reassigned: %s", reassignedPartitions.toString()));
        info(String.format("Resuming reassignment of partitions: %s", partitionsToReassign.toString()));
    }

    private void initializeTopicDeletion() {
        Set<String> topicsQueuedForDeletion = Sc.toSet(ZkUtils.getChildrenParentMayNotExist(zkClient, ZkUtils.DeleteTopicsPath));
        Set<String> topicsWithReplicasOnDeadBrokers = Sc.map(
                Sc.filter(controllerContext.partitionReplicaAssignment, (partition, replicas) ->
                        Sc.exists(replicas, r -> !controllerContext.liveBrokerIds().contains(r)))
                        .keySet(), p -> p.topic);
        Set<String> topicsForWhichPartitionReassignmentIsInProgress = Sc.map(controllerContext.partitionsUndergoingPreferredReplicaElection, p -> p.topic);
        Set<String> topicsForWhichPreferredReplicaElectionIsInProgress = Sc.map(controllerContext.partitionsBeingReassigned.keySet(), p -> p.topic);

        Set<String> topicsIneligibleForDeletion = Sc.add(topicsWithReplicasOnDeadBrokers, topicsForWhichPartitionReassignmentIsInProgress,
                topicsForWhichPreferredReplicaElectionIsInProgress);
        info(String.format("List of topics to be deleted: %s", topicsQueuedForDeletion));
        info(String.format("List of topics ineligible for deletion: %s", topicsIneligibleForDeletion));
        // initialize the topic deletion manager;
        deleteTopicManager = new TopicDeletionManager(this, topicsQueuedForDeletion, topicsIneligibleForDeletion);
    }

    private void maybeTriggerPartitionReassignment() {
        controllerContext.partitionsBeingReassigned.forEach((tp, context) -> initiateReassignReplicasForTopicPartition(tp, context));
    }

    private void maybeTriggerPreferredReplicaElection() {
        onPreferredReplicaElection(Sc.toSet(controllerContext.partitionsUndergoingPreferredReplicaElection));
    }

    private void startChannelManager() {
        controllerContext.controllerChannelManager = new ControllerChannelManager(controllerContext, config);
        controllerContext.controllerChannelManager.startup();
    }

    private void updateLeaderAndIsrCache() throws Throwable {
        Map<TopicAndPartition, LeaderIsrAndControllerEpoch> leaderAndIsrInfo = ZkUtils.getPartitionLeaderAndIsrForTopics(zkClient, controllerContext.partitionReplicaAssignment.keySet());
        leaderAndIsrInfo.forEach((topicPartition, leaderIsrAndControllerEpoch) -> controllerContext.partitionLeadershipInfo.put(topicPartition, leaderIsrAndControllerEpoch));
    }

    private Boolean areReplicasInIsr(String topic, Integer partition, List<Integer> replicas) {
        Optional<LeaderAndIsr> opt = getLeaderAndIsrForPartition(zkClient, topic, partition);
        if (opt.isPresent()) {
            LeaderAndIsr leaderAndIsr = opt.get();
            List<Integer> replicasNotInIsr = Sc.filterNot(replicas, r -> leaderAndIsr.isr.contains(r));
            return replicasNotInIsr.isEmpty();
        } else {
            return false;
        }
    }

    private void moveReassignedPartitionLeaderIfRequired(TopicAndPartition topicAndPartition, ReassignedPartitionsContext reassignedPartitionContext) {
        List<Integer> reassignedReplicas = reassignedPartitionContext.newReplicas;
        Integer currentLeader = controllerContext.partitionLeadershipInfo.get(topicAndPartition).leaderAndIsr.leader;
        // change the assigned replica list to just the reassigned replicas in the cache so it gets sent out on the LeaderAndIsr;
        // request to the current or new leader. This will prevent it from adding the old replicas to the ISR;
        List<Integer> oldAndNewReplicas = controllerContext.partitionReplicaAssignment.get(topicAndPartition);
        controllerContext.partitionReplicaAssignment.put(topicAndPartition, reassignedReplicas);
        if (!reassignedPartitionContext.newReplicas.contains(currentLeader)) {
            info(String.format("Leader %s for partition %s being reassigned, ", currentLeader, topicAndPartition) +
                    String.format("is not in the new list of replicas %s. Re-electing leader", reassignedReplicas));
            // move the leader to one of the alive and caught up new replicas;
            partitionStateMachine.handleStateChanges(Sets.newHashSet(topicAndPartition), OnlinePartition, reassignedPartitionLeaderSelector);
        } else {
            // check if the leader is alive or not;
            if (controllerContext.liveBrokerIds().contains(currentLeader)) {
                info(String.format("Leader %s for partition %s being reassigned, ", currentLeader, topicAndPartition) +
                        String.format("is already in the new list of replicas %s and is alive", reassignedReplicas));
                // shrink replication factor and update the leader epoch in zookeeper to use on the next LeaderAndIsrRequest;
                updateLeaderEpochAndSendRequest(topicAndPartition, oldAndNewReplicas, reassignedReplicas);
            } else {
                info(String.format("Leader %s for partition %s being reassigned, ", currentLeader, topicAndPartition) +
                        String.format("is already in the new list of replicas %s but is dead", reassignedReplicas));
                partitionStateMachine.handleStateChanges(Sets.newHashSet(topicAndPartition), OnlinePartition, reassignedPartitionLeaderSelector);
            }
        }
    }

    private void stopOldReplicasOfReassignedPartition(TopicAndPartition topicAndPartition,
                                                      ReassignedPartitionsContext reassignedPartitionContext,
                                                      Set<Integer> oldReplicas) {
        String topic = topicAndPartition.topic;
        Integer partition = topicAndPartition.partition;
        // first move the replica to offline state (the controller removes it from the ISR);
        Set<PartitionAndReplica> replicasToBeDeleted = Sc.map(oldReplicas, r -> new PartitionAndReplica(topic, partition, r));
        replicaStateMachine.handleStateChanges(replicasToBeDeleted, OfflineReplica);
        // send stop replica command to the old replicas;
        replicaStateMachine.handleStateChanges(replicasToBeDeleted, ReplicaDeletionStarted);
        // Eventually TODO partition reassignment could use a callback that does retries if deletion failed;
        replicaStateMachine.handleStateChanges(replicasToBeDeleted, ReplicaDeletionSuccessful);
        replicaStateMachine.handleStateChanges(replicasToBeDeleted, NonExistentReplica);
    }

    private void updateAssignedReplicasForPartition(TopicAndPartition topicAndPartition, List<Integer> replicas) {
        Map<TopicAndPartition, List<Integer>> partitionsAndReplicasForThisTopic = Sc.filter(controllerContext.partitionReplicaAssignment, (k, v) -> k.topic.equals(topicAndPartition.topic));
        partitionsAndReplicasForThisTopic.put(topicAndPartition, replicas);
        updateAssignedReplicasForPartition(topicAndPartition, partitionsAndReplicasForThisTopic);
        info(String.format("Updated assigned replicas for partition %s being reassigned to %s ", topicAndPartition, replicas));
        // update the assigned replica list after a successful zookeeper write;
        controllerContext.partitionReplicaAssignment.put(topicAndPartition, replicas);
    }

    private void startNewReplicasForReassignedPartition(TopicAndPartition topicAndPartition,
                                                        ReassignedPartitionsContext reassignedPartitionContext,
                                                        Set<Integer> newReplicas) {
        // send the start replica request to the brokers in the reassigned replicas list that are not in the assigned;
        // replicas list;
        newReplicas.forEach(
                replica ->
                        replicaStateMachine.handleStateChanges(Sets.newHashSet(new PartitionAndReplica(topicAndPartition.topic, topicAndPartition.partition, replica)), NewReplica));
    }

    private void updateLeaderEpochAndSendRequest(TopicAndPartition topicAndPartition, List<Integer> replicasToReceiveRequest, List<Integer> newAssignedReplicas) {
        brokerRequestBatch.newBatch();
        Sc.match(updateLeaderEpoch(topicAndPartition.topic, topicAndPartition.partition), updatedLeaderIsrAndControllerEpoch -> {
                    brokerRequestBatch.addLeaderAndIsrRequestForBrokers(replicasToReceiveRequest, topicAndPartition.topic,
                            topicAndPartition.partition, updatedLeaderIsrAndControllerEpoch, newAssignedReplicas);
                    brokerRequestBatch.sendRequestsToBrokers(controllerContext.epoch, controllerContext.correlationId.getAndIncrement());
                    stateChangeLogger.trace(String.format("Controller %d epoch %d sent LeaderAndIsr request %s with new assigned replica list %s " +
                                    "to leader %d for partition being reassigned %s", config.brokerId, controllerContext.epoch, updatedLeaderIsrAndControllerEpoch,
                            newAssignedReplicas, updatedLeaderIsrAndControllerEpoch.leaderAndIsr.leader, topicAndPartition));
                }, () -> stateChangeLogger.error(String.format("Controller %d epoch %d failed to send LeaderAndIsr request with new assigned replica list %s " +
                        "to leader for partition being reassigned %s", config.brokerId, controllerContext.epoch,
                newAssignedReplicas, topicAndPartition))
        );
    }

    private void registerReassignedPartitionsListener() {
        zkClient.subscribeDataChanges(ZkUtils.ReassignPartitionsPath, partitionReassignedListener);
    }

    private void deregisterReassignedPartitionsListener() {
        zkClient.unsubscribeDataChanges(ZkUtils.ReassignPartitionsPath, partitionReassignedListener);
    }

    private void registerPreferredReplicaElectionListener() {
        zkClient.subscribeDataChanges(ZkUtils.PreferredReplicaLeaderElectionPath, preferredReplicaElectionListener);
    }

    private void deregisterPreferredReplicaElectionListener() {
        zkClient.unsubscribeDataChanges(ZkUtils.PreferredReplicaLeaderElectionPath, preferredReplicaElectionListener);
    }

    private void deregisterReassignedPartitionsIsrChangeListeners() {
        controllerContext.partitionsBeingReassigned.forEach((topicAndPartition, reassignedPartitionsContext) -> {
            String zkPartitionPath = ZkUtils.getTopicPartitionLeaderAndIsrPath(topicAndPartition.topic, topicAndPartition.partition);
            zkClient.unsubscribeDataChanges(zkPartitionPath, reassignedPartitionsContext.isrChangeListener);
        });
    }

    private void readControllerEpochFromZookeeper() {
        // initialize the controller epoch and zk version by reading from zookeeper;
        if (ZkUtils.pathExists(controllerContext.zkClient, ZkUtils.ControllerEpochPath)) {
            Tuple<String, Stat> epochData = ZkUtils.readData(controllerContext.zkClient, ZkUtils.ControllerEpochPath);
            controllerContext.epoch = Integer.parseInt(epochData.v1);
            controllerContext.epochZkVersion = epochData.v2.getVersion();
            info(String.format("Initialized controller epoch to %d and zk version %d", controllerContext.epoch, controllerContext.epochZkVersion));
        }
    }

    public void removePartitionFromReassignedPartitions(TopicAndPartition topicAndPartition) {
        if (controllerContext.partitionsBeingReassigned.get(topicAndPartition) != null) {
            // stop watching the ISR changes for this partition;
            zkClient.unsubscribeDataChanges(ZkUtils.getTopicPartitionLeaderAndIsrPath(topicAndPartition.topic, topicAndPartition.partition),
                    controllerContext.partitionsBeingReassigned.get(topicAndPartition).isrChangeListener);
        }
        // read the current list of reassigned partitions from zookeeper;
        Map<TopicAndPartition, ReassignedPartitionsContext> partitionsBeingReassigned = ZkUtils.getPartitionsBeingReassigned(zkClient);
        // remove this partition from that list;
        partitionsBeingReassigned.remove(topicAndPartition);
        Map<TopicAndPartition, ReassignedPartitionsContext> updatedPartitionsBeingReassigned = partitionsBeingReassigned;
        // write the new list to zookeeper;
        ZkUtils.updatePartitionReassignmentData(zkClient, Sc.mapValue(updatedPartitionsBeingReassigned, v -> v.newReplicas));
        // update the cache. NO-OP if the partition's reassignment was never started;
        controllerContext.partitionsBeingReassigned.remove(topicAndPartition);
    }

    public void updateAssignedReplicasForPartition(TopicAndPartition topicAndPartition, Map<TopicAndPartition, List<Integer>> newReplicaAssignmentForTopic) {
        try {
            String zkPath = ZkUtils.getTopicPath(topicAndPartition.topic);
            String jsonPartitionMap = ZkUtils.replicaAssignmentZkData(Sc.mapKey(newReplicaAssignmentForTopic, k -> k.partition.toString()));
            ZkUtils.updatePersistentPath(zkClient, zkPath, jsonPartitionMap);
            debug(String.format("Updated path %s with %s for replica assignment", zkPath, jsonPartitionMap));
        } catch (ZkNoNodeException e) {
            throw new IllegalStateException(String.format("Topic %s doesn't exist", topicAndPartition.topic));
        } catch (Throwable e2) {
            throw new KafkaException(e2.toString());
        }
    }

    public void removePartitionsFromPreferredReplicaElection(Set<TopicAndPartition> partitionsToBeRemoved,
                                                             Boolean isTriggeredByAutoRebalance) {
        for (TopicAndPartition partition : partitionsToBeRemoved) {
            // check the status;
            Integer currentLeader = controllerContext.partitionLeadershipInfo.get(partition).leaderAndIsr.leader;
            Integer preferredReplica = Sc.head(controllerContext.partitionReplicaAssignment.get(partition));
            if (currentLeader == preferredReplica) {
                info(String.format("Partition %s completed preferred replica leader election. New leader is %d", partition, preferredReplica));
            } else {
                warn(String.format("Partition %s failed to complete preferred replica leader election. Leader is %d", partition, currentLeader));
            }
        }
        if (!isTriggeredByAutoRebalance)
            ZkUtils.deletePath(zkClient, ZkUtils.PreferredReplicaLeaderElectionPath);
        controllerContext.partitionsUndergoingPreferredReplicaElection.remove(partitionsToBeRemoved);
    }

    /**
     * Send the leader information for selected partitions to selected brokers so that they can correctly respond to
     * metadata requests
     *
     * @param brokers The brokers that the update metadata request should be sent to
     */
    public void sendUpdateMetadataRequest(List<Integer> brokers) {
        this.sendUpdateMetadataRequest(brokers, Sets.newHashSet());
    }

    public void sendUpdateMetadataRequest(List<Integer> brokers, Set<TopicAndPartition> partitions) {
        brokerRequestBatch.newBatch();
        brokerRequestBatch.addUpdateMetadataRequestForBrokers(brokers, partitions, null);
        brokerRequestBatch.sendRequestsToBrokers(epoch(), controllerContext.correlationId.getAndIncrement());
    }

    /**
     * Removes a given partition replica from the ISR; if it is not the current
     * leader and there are sufficient remaining replicas in ISR.
     *
     * @param topic     topic
     * @param partition partition
     * @param replicaId replica Id
     * @return the new leaderAndIsr (with the replica removed if it was present),
     * or None if leaderAndIsr is empty.
     */
    public Optional<LeaderIsrAndControllerEpoch> removeReplicaFromIsr(String topic, Integer partition, Integer replicaId) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        debug(String.format("Removing replica %d from ISR %s for partition %s.", replicaId,
                controllerContext.partitionLeadershipInfo.get(topicAndPartition).leaderAndIsr.isr, topicAndPartition));
        Optional<LeaderIsrAndControllerEpoch> finalLeaderIsrAndControllerEpoch = Optional.empty();
        boolean zkWriteCompleteOrUnnecessary = false;
        while (!zkWriteCompleteOrUnnecessary) {
            // refresh leader and isr from zookeeper again;
            Optional<LeaderIsrAndControllerEpoch> leaderIsrAndEpochOpt = ReplicationUtils.getLeaderIsrAndEpochForPartition(zkClient, topic, partition);
            if (leaderIsrAndEpochOpt.isPresent()) {
                LeaderIsrAndControllerEpoch leaderIsrAndEpoch = leaderIsrAndEpochOpt.get();
                LeaderAndIsr leaderAndIsr = leaderIsrAndEpoch.leaderAndIsr;
                Integer controllerEpoch = leaderIsrAndEpoch.controllerEpoch;
                if (controllerEpoch > epoch())
                    throw new StateChangeFailedException("Leader and isr path written by another controller. This probably" +
                            String.format("means the current controller with epoch %d went through a soft failure and another ", epoch()) +
                            String.format("controller was elected with epoch %d. Aborting state change by this controller", controllerEpoch));
                if (leaderAndIsr.isr.contains(replicaId)) {
                    // if the replica to be removed from the ISR is also the leader, set the new leader value to -1;
                    Integer newLeader = (replicaId == leaderAndIsr.leader) ? LeaderAndIsr.NoLeader : leaderAndIsr.leader;
                    List<Integer> newIsr = Sc.filter(leaderAndIsr.isr, b -> b != replicaId);

                    // if the replica to be removed from the ISR is the last surviving member of the ISR and unclean leader election;
                    // is disallowed for the corresponding topic, then we must preserve the ISR membership so that the replica can;
                    // eventually be restored as the leader.;
                    if (newIsr.isEmpty() && !LogConfig.fromProps(config.props.props, AdminUtils.fetchTopicConfig(zkClient,
                            topicAndPartition.topic)).uncleanLeaderElectionEnable) {
                        info(String.format("Retaining last ISR %d of partition %s since unclean leader election is disabled", replicaId, topicAndPartition));
                        newIsr = leaderAndIsr.isr;
                    }

                    LeaderAndIsr newLeaderAndIsr = new LeaderAndIsr(newLeader, leaderAndIsr.leaderEpoch + 1,
                            newIsr, leaderAndIsr.zkVersion + 1);
                    // update the new leadership decision in zookeeper or retry;
                    Tuple<Boolean, Integer> tuple = ReplicationUtils.updateLeaderAndIsr(zkClient, topic, partition,
                            newLeaderAndIsr, epoch(), leaderAndIsr.zkVersion);
                    Boolean updateSucceeded = tuple.v1;
                    Integer newVersion = tuple.v2;

                    newLeaderAndIsr.zkVersion = newVersion;
                    finalLeaderIsrAndControllerEpoch = Optional.of(new LeaderIsrAndControllerEpoch(newLeaderAndIsr, epoch()));
                    controllerContext.partitionLeadershipInfo.put(topicAndPartition, finalLeaderIsrAndControllerEpoch.get());
                    if (updateSucceeded)
                        info(String.format("New leader and ISR for partition %s is %s", topicAndPartition, newLeaderAndIsr.toString()));
                    zkWriteCompleteOrUnnecessary = updateSucceeded;
                } else {
                    warn(String.format("Cannot remove replica %d from ISR of partition %s since it is not in the ISR. Leader = %d ; ISR = %s",
                            replicaId, topicAndPartition, leaderAndIsr.leader, leaderAndIsr.isr));
                    finalLeaderIsrAndControllerEpoch = Optional.of(new LeaderIsrAndControllerEpoch(leaderAndIsr, epoch()));
                    controllerContext.partitionLeadershipInfo.put(topicAndPartition, finalLeaderIsrAndControllerEpoch.get());
                    zkWriteCompleteOrUnnecessary = true;
                }
            } else {
                warn(String.format("Cannot remove replica %d from ISR of %s - leaderAndIsr is empty.", replicaId, topicAndPartition));
                zkWriteCompleteOrUnnecessary = true;
            }
        }
        return finalLeaderIsrAndControllerEpoch;
    }

    /**
     * Does not change leader or isr, but just increments the leader epoch
     *
     * @param topic     topic
     * @param partition partition
     * @return the new leaderAndIsr with an incremented leader epoch, or None if leaderAndIsr is empty.
     */
    private Optional<LeaderIsrAndControllerEpoch> updateLeaderEpoch(String topic, Integer partition) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        debug(String.format("Updating leader epoch for partition %s.", topicAndPartition));
        Optional<LeaderIsrAndControllerEpoch> finalLeaderIsrAndControllerEpoch = Optional.empty();
        boolean zkWriteCompleteOrUnnecessary = false;
        while (!zkWriteCompleteOrUnnecessary) {
            // refresh leader and isr from zookeeper again;
            Optional<LeaderIsrAndControllerEpoch> leaderIsrAndEpochOpt = ReplicationUtils.getLeaderIsrAndEpochForPartition(zkClient, topic, partition);
            LeaderIsrAndControllerEpoch leaderIsrAndEpoch = leaderIsrAndEpochOpt.get();
            if (leaderIsrAndEpochOpt.isPresent()) {
                LeaderAndIsr leaderAndIsr = leaderIsrAndEpoch.leaderAndIsr;
                Integer controllerEpoch = leaderIsrAndEpoch.controllerEpoch;
                if (controllerEpoch > epoch())
                    throw new StateChangeFailedException("Leader and isr path written by another controller. This probably" +
                            String.format("means the current controller with epoch %d went through a soft failure and another ", epoch()) +
                            String.format("controller was elected with epoch %d. Aborting state change by this controller", controllerEpoch));
                // increment the leader epoch even if there are no leader or isr changes to allow the leader to cache the expanded;
                // assigned replica list;
                LeaderAndIsr newLeaderAndIsr = new LeaderAndIsr(leaderAndIsr.leader, leaderAndIsr.leaderEpoch + 1,
                        leaderAndIsr.isr, leaderAndIsr.zkVersion + 1);
                // update the new leadership decision in zookeeper or retry;

                Tuple<Boolean, Integer> tuple = ReplicationUtils.updateLeaderAndIsr(zkClient, topic,
                        partition, newLeaderAndIsr, epoch(), leaderAndIsr.zkVersion);
                boolean updateSucceeded = tuple.v1;
                Integer newVersion = tuple.v2;
                newLeaderAndIsr.zkVersion = newVersion;
                finalLeaderIsrAndControllerEpoch = Optional.of(new LeaderIsrAndControllerEpoch(newLeaderAndIsr, epoch()));
                if (updateSucceeded)
                    info(String.format("Updated leader epoch for partition %s to %d", topicAndPartition, newLeaderAndIsr.leaderEpoch));
                zkWriteCompleteOrUnnecessary = updateSucceeded;
            } else {
                throw new IllegalStateException(String.format("Cannot update leader epoch for partition %s as leaderAndIsr path is empty. " +
                        "This could mean we somehow tried to reassign a partition that doesn't exist", topicAndPartition));
            }

        }
        return finalLeaderIsrAndControllerEpoch;
    }

    // TODO: 2017/11/15 是否没用？？？
    public Action checkAndTriggerPartitionRebalance = () -> {
        if (isActive()) {
            trace("checking need to trigger partition rebalance");
            // get all the active brokers;
            Map<Integer, Map<TopicAndPartition, List<Integer>>> preferredReplicasForTopicsByBrokers = Utils.inLock(controllerContext.controllerLock, () -> {
                Map<TopicAndPartition, List<Integer>> t = Sc.filterNot(controllerContext.partitionReplicaAssignment,
                        (k, v) -> deleteTopicManager.isTopicQueuedUpForDeletion(k.topic));
                return Sc.groupByValue(t, assignedReplicas -> Sc.head(assignedReplicas));
            });
            debug("preferred replicas by broker " + preferredReplicasForTopicsByBrokers);
            // for each broker, check if a preferred replica election needs to be triggered;
            preferredReplicasForTopicsByBrokers.forEach((leaderBroker, topicAndPartitionsForBroker) -> {
                NumCount<Double> imbalanceRatio = NumCount.of(0D);
                Map<TopicAndPartition, List<Integer>> topicsNotInPreferredReplica = Utils.inLock(controllerContext.controllerLock, () -> {
                    Map<TopicAndPartition, List<Integer>> topicsNotInPreferredReplica2 =
                            Sc.filter(topicAndPartitionsForBroker, (topicPartition, replicas) ->
                                    controllerContext.partitionLeadershipInfo.containsKey(topicPartition) &&
                                            controllerContext.partitionLeadershipInfo.get(topicPartition).leaderAndIsr.leader != leaderBroker);
                    debug("topics not in preferred replica " + topicsNotInPreferredReplica2);
                    int totalTopicPartitionsForBroker = topicAndPartitionsForBroker.size();
                    int totalTopicPartitionsNotLedByBroker = topicsNotInPreferredReplica2.size();
                    imbalanceRatio.set((double) totalTopicPartitionsNotLedByBroker / totalTopicPartitionsForBroker);
                    trace(String.format("leader imbalance ratio for broker %d is %f", leaderBroker, imbalanceRatio.get()));
                    return topicsNotInPreferredReplica2;
                });
                // check ratio and if greater than desired ratio, trigger a rebalance for the topic partitions;
                // that need to be on this broker;
                if (imbalanceRatio.get() > ((double) config.leaderImbalancePerBrokerPercentage / 100)) {
                    topicsNotInPreferredReplica.forEach((topicPartition, replicas) -> {
                        Utils.inLock(controllerContext.controllerLock, () -> {
                            // do this check only if the broker is live and there are no partitions being reassigned currently;
                            // and preferred replica election is not in progress;
                            if (controllerContext.liveBrokerIds().contains(leaderBroker) &&
                                    controllerContext.partitionsBeingReassigned.size() == 0 &&
                                    controllerContext.partitionsUndergoingPreferredReplicaElection.size() == 0 &&
                                    !deleteTopicManager.isTopicQueuedUpForDeletion(topicPartition.topic) &&
                                    controllerContext.allTopics.contains(topicPartition.topic)) {
                                onPreferredReplicaElection(Sets.newHashSet(topicPartition), true);
                            }
                        });
                    });
                }
            });
        }
    };

    class SessionExpirationListener extends Logging implements IZkStateListener {
        public SessionExpirationListener() {
            this.logIdent = "<SessionExpirationListener on " + config.brokerId + ">, ";
        }

        //        @throws(classOf<Exception>)
        public void handleStateChanged(Watcher.Event.KeeperState state) {
            // do nothing, since zkclient will do reconnect for us.;
        }

        /**
         * Called after the zookeeper session has expired and a new session has been created. You would have to re-create
         * any ephemeral nodes here.
         *
         * @throws Exception On any error.
         */
//        @throws(classOf<Exception>)
        public void handleNewSession() {
            info("ZK expired; shut down all controller components and try to re-elect");
            Utils.inLock(controllerContext.controllerLock, () -> {
                onControllerResignation.invoke();
                controllerElector.elect();
            });
        }
    }


}

/**
 * Starts the partition reassignment process unless -
 * 1. Partition previously existed
 * 2. New replicas are the same as existing replicas
 * 3. Object replica in the new set of replicas are dead
 * If any of the above conditions are satisfied, it logs an error and removes the partition from list of reassigned
 * partitions.
 */
class PartitionsReassignedListener extends Logging implements IZkDataListener {
    public KafkaController controller;
    public ZkClient zkClient;
    public ControllerContext controllerContext;

    public PartitionsReassignedListener(KafkaController controller) {
        this.controller = controller;
        this.logIdent = "<PartitionsReassignedListener on " + controller.config.brokerId + ">: ";
        zkClient = controller.controllerContext.zkClient;
        controllerContext = controller.controllerContext;

    }

    /**
     * Invoked when some partitions are reassigned by the admin command
     *
     * @throws Exception On any error.
     */
//        @throws(classOf<Exception>)
    public void handleDataChange(String dataPath, Object data) {
        debug(String.format("Partitions reassigned listener fired for path %s. Record partitions to be reassigned %s", dataPath, data));
        Map<TopicAndPartition, List<Integer>> partitionsReassignmentData = ZkUtils.parsePartitionReassignmentData(data.toString());
        Map<TopicAndPartition, List<Integer>> partitionsToBeReassigned = Utils.inLock(controllerContext.controllerLock, () ->
                Sc.filterNot(partitionsReassignmentData, (k, v) -> controllerContext.partitionsBeingReassigned.containsKey(k))
        );
        partitionsToBeReassigned.forEach(
                (k, v) ->
                        Utils.inLock(controllerContext.controllerLock, () -> {
                            if (controller.deleteTopicManager.isTopicQueuedUpForDeletion(k.topic)) {
                                error(String.format("Skipping reassignment of partition %s for topic %s since it is currently being deleted",
                                        k, k.topic));
                                controller.removePartitionFromReassignedPartitions(k);
                            } else {
                                ReassignedPartitionsContext context = new ReassignedPartitionsContext(v);
                                controller.initiateReassignReplicasForTopicPartition(k, context);
                            }
                        }));
    }

    /**
     * Called when the leader information stored in zookeeper has been delete. Try to elect as the leader
     *
     * @throws Exception On any error.
     */
//        @throws(classOf<Exception>)
    public void handleDataDeleted(String dataPath) {
    }
}


/**
 * Starts the preferred replica leader election for the list of partitions specified under
 * /admin/preferred_replica_election -
 */
class PreferredReplicaElectionListener extends Logging implements IZkDataListener {
    public KafkaController controller;
    public ZkClient zkClient;
    public ControllerContext controllerContext;

    public PreferredReplicaElectionListener(KafkaController controller) {
        this.controller = controller;
        this.logIdent = "<PreferredReplicaElectionListener on " + controller.config.brokerId + ">: ";
        zkClient = controller.controllerContext.zkClient;
        controllerContext = controller.controllerContext;
    }


    /**
     * Invoked when some partitions are reassigned by the admin command
     *
     * @throws Exception On any error.
     */
//        @throws(classOf<Exception>)
    public void handleDataChange(String dataPath, Object data) {
        debug(String.format("Preferred replica election listener fired for path %s. Record partitions to undergo preferred replica election %s", dataPath, data));
        Utils.inLock(controllerContext.controllerLock, () -> {
            Set<TopicAndPartition> partitionsForPreferredReplicaElection = PreferredReplicaLeaderElectionCommand.parsePreferredReplicaElectionData(data.toString());
            if (controllerContext.partitionsUndergoingPreferredReplicaElection.size() > 0)
                info(String.format("These partitions are already undergoing preferred replica election: %s", controllerContext.partitionsUndergoingPreferredReplicaElection));
            Set<TopicAndPartition> partitions = Sc.subtract(partitionsForPreferredReplicaElection, controllerContext.partitionsUndergoingPreferredReplicaElection);
            Set<TopicAndPartition> partitionsForTopicsToBeDeleted = Sc.filter(partitions, p -> controller.deleteTopicManager.isTopicQueuedUpForDeletion(p.topic));
            if (partitionsForTopicsToBeDeleted.size() > 0) {
                error(String.format("Skipping preferred replica election for partitions %s since the respective topics are being deleted",
                        partitionsForTopicsToBeDeleted));
            }
            controller.onPreferredReplicaElection(Sc.subtract(partitions, partitionsForTopicsToBeDeleted));
        });
    }

    /**
     * @throws Exception On any error.
     */
//        @throws(classOf<Exception>)
    public void handleDataDeleted(String dataPath) {
    }
}

