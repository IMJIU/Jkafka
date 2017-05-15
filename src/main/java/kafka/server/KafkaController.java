package kafka.server;/**
 * Created by zhoulf on 2017/5/15.
 */

import com.alibaba.fastjson.JSON;
import kafka.common.KafkaException;
import kafka.metrics.KafkaMetricsGroup;
import kafka.utils.Logging;
import org.I0Itec.zkclient.ZkClient;

import java.util.Map;

/**
 * @author
 * @create 2017-05-15 47 15
 **/
public class KafkaController extends KafkaMetricsGroup{
    public KafkaConfig config;
    public ZkClient zkClient;
    public  BrokerState brokerState;
    public static Logging log = new Logging();

    public KafkaController(KafkaConfig config, ZkClient zkClient, BrokerState brokerState) {
        this.config = config;
        this.zkClient = zkClient;
        this.brokerState = brokerState;
        loggerName("<Controller " + config.brokerId + ">: ");
    }
        private Boolean isRunning = true;
//        private val stateChangeLogger = KafkaController.stateChangeLogger;
//    ControllerContext controllerContext = new ControllerContext(zkClient, config.zkSessionTimeoutMs);
//    PartitionStateMachine partitionStateMachine = new PartitionStateMachine(this);
//    ReplicaStateMachine replicaStateMachine = new ReplicaStateMachine(this);
//        private ZookeeperLeaderElector controllerElector = new ZookeeperLeaderElector(controllerContext, ZkUtils.ControllerPath, onControllerFailover,
//                onControllerResignation, config.brokerId);
//        // have a separate scheduler for the controller to be able to start and stop independently of the;
//        // kafka server;
//        private KafkaScheduler autoRebalanceScheduler = new KafkaScheduler(1);
//         TopicDeletionManager deleteTopicManager = null;
//    OfflinePartitionLeaderSelector offlinePartitionSelector = new OfflinePartitionLeaderSelector(controllerContext, config);
//        private ReassignedPartitionLeaderSelector reassignedPartitionLeaderSelector = new ReassignedPartitionLeaderSelector(controllerContext);
//        private PreferredReplicaPartitionLeaderSelector preferredReplicaPartitionLeaderSelector = new PreferredReplicaPartitionLeaderSelector(controllerContext);
//        private ControlledShutdownLeaderSelector controlledShutdownPartitionLeaderSelector = new ControlledShutdownLeaderSelector(controllerContext);
//        private ControllerBrokerRequestBatch brokerRequestBatch = new ControllerBrokerRequestBatch(this);
//
//        private PartitionsReassignedListener partitionReassignedListener = new PartitionsReassignedListener(this);
//        private PreferredReplicaElectionListener preferredReplicaElectionListener = new PreferredReplicaElectionListener(this);

//        newGauge(
//                "ActiveControllerCount",
//                new Gauge<Integer> {
//           public void value() = if (isActive) 1 else 0;
//        }
//  );
//
//        newGauge(
//                "OfflinePartitionsCount",
//                new Gauge<Integer> {
//           public Integer  void value() {
//                    inLock(controllerContext.controllerLock) {
//            if (!isActive())
//                0;
//            else;
//                controllerContext.partitionLeadershipInfo.count(p => !controllerContext.liveOrShuttingDownBrokerIds.contains(p._2.leaderAndIsr.leader));
//        }
//      }
//        }
//  );
//
//        newGauge(
//                "PreferredReplicaImbalanceCount",
//                new Gauge<Integer> {
//           public Integer  void value() {
//                    inLock(controllerContext.controllerLock) {
//            if (!isActive())
//                0;
//            else;
//                controllerContext.partitionReplicaAssignment.count {
//                case (topicPartition, replicas) => controllerContext.partitionLeadershipInfo(topicPartition).leaderAndIsr.leader != replicas.head;
//            }
//        }
//      }
//        }
//  );
    public static StateChangeLogger stateChangeLogger = new StateChangeLogger("state.change.logger");
    public static  final Integer InitialControllerEpoch = 1;
    public static   final Integer InitialControllerEpochZkVersion = 1;



    public static Integer parseControllerId(String controllerInfoString ) {
        try {
            Map<String, Object> m = JSON.parseObject(controllerInfoString, Map.class);
            if (m != null) {
                return Integer.parseInt(m.get("brokerid").toString());
            } else {
                throw new KafkaException("Failed to parse the controller info json [%s].".format(controllerInfoString));
            }
        } catch (Throwable e) {
            // It may be due to an incompatible controller register version
            log.warn("Failed to parse the controller info as json. "
                    + "Probably this controller is still using the old format [%s] to store the broker id in zookeeper".format(controllerInfoString));
            try {
                return Integer.parseInt(controllerInfoString);
            } catch (Throwable t) {
                throw new KafkaException("Failed to parse the controller info: " + controllerInfoString + ". This is neither the new or the old format.", t);
            }
        }
    }
//       public void epoch = controllerContext.epoch;
//
//       public void clientId = String.format("id_%d-host_%s-port_%d",config.brokerId, config.hostName, config.port)
//
//        /**
//         * On clean shutdown, the controller first determines the partitions that the
//         * shutting down broker leads, and moves leadership of those partitions to another broker
//         * that is in that partition's ISR.
//         *
//         * @param id Id of the broker to shutdown.
//         * @return The number of partitions that the broker still leads.
//         */
//       public void shutdownBroker(Int id) : Set<TopicAndPartition> = {
//
//        if (!isActive()) {
//            throw new ControllerMovedException("Controller moved to another broker. Aborting controlled shutdown");
//        }
//
//        controllerContext.brokerShutdownLock synchronized {
//            info("Shutting down broker " + id);
//
//            inLock(controllerContext.controllerLock) {
//                if (!controllerContext.liveOrShuttingDownBrokerIds.contains(id))
//                    throw new BrokerNotAvailableException(String.format("Broker id %d does not exist.",id))
//
//                controllerContext.shuttingDownBrokerIds.add(id);
//                debug("All shutting down brokers: " + controllerContext.shuttingDownBrokerIds.mkString(","));
//                debug("Live brokers: " + controllerContext.liveBrokerIds.mkString(","));
//            }
//
//            val Set allPartitionsAndReplicationFactorOnBroker<(TopicAndPartition, Int)> =
//            inLock(controllerContext.controllerLock) {
//                controllerContext.partitionsOnBroker(id);
//                        .map(topicAndPartition => (topicAndPartition, controllerContext.partitionReplicaAssignment(topicAndPartition).size));
//            }
//
//            allPartitionsAndReplicationFactorOnBroker.foreach {
//                case(topicAndPartition, replicationFactor) =>
//                    // Move leadership serially to relinquish lock.;
//                    inLock(controllerContext.controllerLock) {
//                    controllerContext.partitionLeadershipInfo.get(topicAndPartition).foreach { currLeaderIsrAndControllerEpoch =>
//                        if (replicationFactor > 1) {
//                            if (currLeaderIsrAndControllerEpoch.leaderAndIsr.leader == id) {
//                                // If the broker leads the topic partition, transition the leader and update isr. Updates zk and;
//                                // notifies all affected brokers;
//                                partitionStateMachine.handleStateChanges(Set(topicAndPartition), OnlinePartition,
//                                        controlledShutdownPartitionLeaderSelector);
//                            } else {
//                                // Stop the replica first. The state change below initiates ZK changes which should take some time;
//                                // before which the stop replica request should be completed (in most cases)
//                                brokerRequestBatch.newBatch();
//                                brokerRequestBatch.addStopReplicaRequestForBrokers(Seq(id), topicAndPartition.topic,
//                                        topicAndPartition.partition, deletePartition = false);
//                                brokerRequestBatch.sendRequestsToBrokers(epoch, controllerContext.correlationId.getAndIncrement);
//
//                                // If the broker is a follower, updates the isr in ZK and notifies the current leader;
//                                replicaStateMachine.handleStateChanges(Set(PartitionAndReplica(topicAndPartition.topic,
//                                        topicAndPartition.partition, id)), OfflineReplica);
//                            }
//                        }
//                    }
//                }
//            }
//           public void replicatedPartitionsBrokerLeads() = inLock(controllerContext.controllerLock) {
//                trace("All leaders = " + controllerContext.partitionLeadershipInfo.mkString(","));
//                controllerContext.partitionLeadershipInfo.filter {
//                    case (topicAndPartition, leaderIsrAndControllerEpoch) =>
//                        leaderIsrAndControllerEpoch.leaderAndIsr.leader == id && controllerContext.partitionReplicaAssignment(topicAndPartition).size > 1;
//                }.map(_._1);
//            }
//            replicatedPartitionsBrokerLeads().toSet;
//        }
//  }
//
//        /**
//         * This callback is invoked by the zookeeper leader elector on electing the current broker as the new controller.
//         * It does the following things on the become-controller state change -
//         * 1. Register controller epoch changed listener
//         * 2. Increments the controller epoch
//         * 3. Initializes the controller's context object that holds cache objects for current topics, live brokers and
//         *    leaders for all existing partitions.
//         * 4. Starts the controller's channel manager
//         * 5. Starts the replica state machine
//         * 6. Starts the partition state machine
//         * If it encounters any unexpected exception/error while becoming controller, it resigns as the current controller.
//         * This ensures another controller election will be triggered and there will always be an actively serving controller
//         */
//       public void onControllerFailover() {
//            if(isRunning) {
//                info(String.format("Broker %d starting become controller state transition",config.brokerId))
//                //read controller epoch from zk;
//                readControllerEpochFromZookeeper();
//                // increment the controller epoch;
//                incrementControllerEpoch(zkClient);
//                // before reading source of truth from zookeeper, register the listeners to get broker/topic callbacks;
//                registerReassignedPartitionsListener();
//                registerPreferredReplicaElectionListener();
//                partitionStateMachine.registerListeners();
//                replicaStateMachine.registerListeners();
//                initializeControllerContext();
//                replicaStateMachine.startup();
//                partitionStateMachine.startup();
//                // register the partition change listeners for all existing topics on failover;
//                controllerContext.allTopics.foreach(topic => partitionStateMachine.registerPartitionChangeListener(topic))
//                info(String.format("Broker %d is ready to serve as the new controller with epoch %d",config.brokerId, epoch))
//                brokerState.newState(RunningAsController);
//                maybeTriggerPartitionReassignment();
//                maybeTriggerPreferredReplicaElection();
//      /* send partition leadership info to all live brokers */
//                sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq);
//                if (config.autoLeaderRebalanceEnable) {
//                    info("starting the partition rebalance scheduler");
//                    autoRebalanceScheduler.startup();
//                    autoRebalanceScheduler.schedule("partition-rebalance-thread", checkAndTriggerPartitionRebalance,
//                            5, config.leaderImbalanceCheckIntervalSeconds, TimeUnit.SECONDS);
//                }
//                deleteTopicManager.start();
//            }
//            else;
//                info("Controller has been shut down, aborting startup/failover");
//        }
//
//        /**
//         * This callback is invoked by the zookeeper leader elector when the current broker resigns as the controller. This is
//         * required to clean up internal controller data structures
//         */
//       public void onControllerResignation() {
//            // de-register listeners;
//            deregisterReassignedPartitionsListener();
//            deregisterPreferredReplicaElectionListener();
//
//            // shutdown delete topic manager;
//            if (deleteTopicManager != null)
//                deleteTopicManager.shutdown();
//
//            // shutdown leader rebalance scheduler;
//            if (config.autoLeaderRebalanceEnable)
//                autoRebalanceScheduler.shutdown();
//
//            inLock(controllerContext.controllerLock) {
//                // de-register partition ISR listener for on-going partition reassignment task;
//                deregisterReassignedPartitionsIsrChangeListeners();
//                // shutdown partition state machine;
//                partitionStateMachine.shutdown();
//                // shutdown replica state machine;
//                replicaStateMachine.shutdown();
//                // shutdown controller channel manager;
//                if(controllerContext.controllerChannelManager != null) {
//                    controllerContext.controllerChannelManager.shutdown();
//                    controllerContext.controllerChannelManager = null;
//                }
//                // reset controller context;
//                controllerContext.epoch=0;
//                controllerContext.epochZkVersion=0;
//                brokerState.newState(RunningAsBroker);
//            }
//        }
//
//        /**
//         * Returns true if this broker is the current controller.
//         */
//       public Boolean  void isActive() {
//                inLock(controllerContext.controllerLock) {
//                controllerContext.controllerChannelManager != null;
//        }
//        }
//
//        /**
//         * This callback is invoked by the replica state machine's broker change listener, with the list of newly started
//         * brokers as input. It does the following -
//         * 1. Triggers the OnlinePartition state change for all new/offline partitions
//         * 2. It checks whether there are reassigned replicas assigned to any newly started brokers.  If
//         *    so, it performs the reassignment logic for each topic/partition.
//         *
//         * Note that we don't need to refresh the leader/isr cache for all topic/partitions at this point for two reasons:
//         * 1. The partition state machine, when triggering online state change, will refresh leader and ISR for only those
//         *    partitions currently new or offline (rather than every partition this controller is aware of)
//         * 2. Even if we do refresh the cache, there is no guarantee that by the time the leader and ISR request reaches
//         *    every broker that it is still valid.  Brokers check the leader epoch to determine validity of the request.
//         */
//       public void onBrokerStartup(Seq newBrokers<Integer>) {
//            info(String.format("New broker startup callback for %s",newBrokers.mkString(",")))
//            val newBrokersSet = newBrokers.toSet;
//            // send update metadata request for all partitions to the newly restarted brokers. In cases of controlled shutdown;
//            // leaders will not be elected when a new broker comes up. So at least in the common controlled shutdown case, the;
//            // metadata will reach the new brokers faster;
//            sendUpdateMetadataRequest(newBrokers);
//            // the very first thing to do when a new broker comes up is send it the entire list of partitions that it is;
//            // supposed to host. Based on that the broker starts the high watermark threads for the input list of partitions;
//            val allReplicasOnNewBrokers = controllerContext.replicasOnBrokers(newBrokersSet);
//            replicaStateMachine.handleStateChanges(allReplicasOnNewBrokers, OnlineReplica);
//            // when a new broker comes up, the controller needs to trigger leader election for all new and offline partitions;
//            // to see if these brokers can become leaders for some/all of those;
//            partitionStateMachine.triggerOnlinePartitionStateChange();
//            // check if reassignment of some partitions need to be restarted;
//            val partitionsWithReplicasOnNewBrokers = controllerContext.partitionsBeingReassigned.filter {
//                case (topicAndPartition, reassignmentContext) => reassignmentContext.newReplicas.exists(newBrokersSet.contains(_));
//            }
//            partitionsWithReplicasOnNewBrokers.foreach(p => onPartitionReassignment(p._1, p._2))
//            // check if topic deletion needs to be resumed. If at least one replica that belongs to the topic being deleted exists;
//            // on the newly restarted brokers, there is a chance that topic deletion can resume;
//            val replicasForTopicsToBeDeleted = allReplicasOnNewBrokers.filter(p => deleteTopicManager.isTopicQueuedUpForDeletion(p.topic));
//            if(replicasForTopicsToBeDeleted.size > 0) {
//                info(("Some replicas %s for topics scheduled for deletion %s are on the newly restarted brokers %s. " +
//                        "Signaling restart of topic deletion for these topics").format(replicasForTopicsToBeDeleted.mkString(","),
//                        deleteTopicManager.topicsToBeDeleted.mkString(","), newBrokers.mkString(",")));
//                deleteTopicManager.resumeDeletionForTopics(replicasForTopicsToBeDeleted.map(_.topic));
//            }
//        }
//
//        /**
//         * This callback is invoked by the replica state machine's broker change listener with the list of failed brokers
//         * as input. It does the following -
//         * 1. Mark partitions with dead leaders as offline
//         * 2. Triggers the OnlinePartition state change for all new/offline partitions
//         * 3. Invokes the OfflineReplica state change on the input list of newly started brokers
//         *
//         * Note that we don't need to refresh the leader/isr cache for all topic/partitions at this point.  This is because
//         * the partition state machine will refresh our cache for us when performing leader election for all new/offline
//         * partitions coming online.
//         */
//       public void onBrokerFailure(Seq deadBrokers<Integer>) {
//            info(String.format("Broker failure callback for %s",deadBrokers.mkString(",")))
//            val deadBrokersThatWereShuttingDown =
//                    deadBrokers.filter(id => controllerContext.shuttingDownBrokerIds.remove(id));
//            info(String.format("Removed %s from list of shutting down brokers.",deadBrokersThatWereShuttingDown))
//            val deadBrokersSet = deadBrokers.toSet;
//            // trigger OfflinePartition state for all partitions whose current leader is one amongst the dead brokers;
//            val partitionsWithoutLeader = controllerContext.partitionLeadershipInfo.filter(partitionAndLeader =>
//                    deadBrokersSet.contains(partitionAndLeader._2.leaderAndIsr.leader) &&;
//                    !deleteTopicManager.isTopicQueuedUpForDeletion(partitionAndLeader._1.topic)).keySet;
//            partitionStateMachine.handleStateChanges(partitionsWithoutLeader, OfflinePartition);
//            // trigger OnlinePartition state changes for offline or new partitions;
//            partitionStateMachine.triggerOnlinePartitionStateChange();
//            // filter out the replicas that belong to topics that are being deleted;
//            var allReplicasOnDeadBrokers = controllerContext.replicasOnBrokers(deadBrokersSet);
//            val activeReplicasOnDeadBrokers = allReplicasOnDeadBrokers.filterNot(p => deleteTopicManager.isTopicQueuedUpForDeletion(p.topic));
//            // handle dead replicas;
//            replicaStateMachine.handleStateChanges(activeReplicasOnDeadBrokers, OfflineReplica);
//            // check if topic deletion state for the dead replicas needs to be updated;
//            val replicasForTopicsToBeDeleted = allReplicasOnDeadBrokers.filter(p => deleteTopicManager.isTopicQueuedUpForDeletion(p.topic));
//            if(replicasForTopicsToBeDeleted.size > 0) {
//                // it is required to mark the respective replicas in TopicDeletionFailed state since the replica cannot be;
//                // deleted when the broker is down. This will prevent the replica from being in TopicDeletionStarted state indefinitely;
//                // since topic deletion cannot be retried until at least one replica is in TopicDeletionStarted state;
//                deleteTopicManager.failReplicaDeletion(replicasForTopicsToBeDeleted);
//            }
//        }
//
//        /**
//         * This callback is invoked by the partition state machine's topic change listener with the list of new topics
//         * and partitions as input. It does the following -
//         * 1. Registers partition change listener. This is not required until KAFKA-347
//         * 2. Invokes the new partition callback
//         * 3. Send metadata request with the new topic to all brokers so they allow requests for that topic to be served
//         */
//       public void onNewTopicCreation(Set topics<String], Set newPartitions[TopicAndPartition>) {
//            info(String.format("New topic creation callback for %s",newPartitions.mkString(",")))
//            // subscribe to partition changes;
//            topics.foreach(topic => partitionStateMachine.registerPartitionChangeListener(topic))
//            onNewPartitionCreation(newPartitions);
//        }
//
//        /**
//         * This callback is invoked by the topic change callback with the list of failed brokers as input.
//         * It does the following -
//         * 1. Move the newly created partitions to the NewPartition state
//         * 2. Move the newly created partitions from NewPartition->OnlinePartition state
//         */
//       public void onNewPartitionCreation(Set newPartitions<TopicAndPartition>) {
//            info(String.format("New partition creation callback for %s",newPartitions.mkString(",")))
//            partitionStateMachine.handleStateChanges(newPartitions, NewPartition);
//            replicaStateMachine.handleStateChanges(controllerContext.replicasForPartition(newPartitions), NewReplica);
//            partitionStateMachine.handleStateChanges(newPartitions, OnlinePartition, offlinePartitionSelector);
//            replicaStateMachine.handleStateChanges(controllerContext.replicasForPartition(newPartitions), OnlineReplica);
//        }
//
//        /**
//         * This callback is invoked by the reassigned partitions listener. When an admin command initiates a partition
//         * reassignment, it creates the /admin/reassign_partitions path that triggers the zookeeper listener.
//         * Reassigning replicas for a partition goes through a few steps listed in the code.
//         * RAR = Reassigned replicas
//         * OAR = Original list of replicas for partition
//         * AR = current assigned replicas
//         *
//         * 1. Update AR in ZK with OAR + RAR.
//         * 2. Send LeaderAndIsr request to every replica in OAR + RAR (with AR as OAR + RAR). We do this by forcing an update
//         *    of the leader epoch in zookeeper.
//         * 3. Start new replicas RAR - OAR by moving replicas in RAR - OAR to NewReplica state.
//         * 4. Wait until all replicas in RAR are in sync with the leader.
//         * 5  Move all replicas in RAR to OnlineReplica state.
//         * 6. Set AR to RAR in memory.
//         * 7. If the leader is not in RAR, elect a new leader from RAR. If new leader needs to be elected from RAR, a LeaderAndIsr
//         *    will be sent. If not, then leader epoch will be incremented in zookeeper and a LeaderAndIsr request will be sent.
//         *    In any case, the LeaderAndIsr request will have AR = RAR. This will prevent the leader from adding any replica in
//         *    RAR - OAR back in the isr.
//         * 8. Move all replicas in OAR - RAR to OfflineReplica state. As part of OfflineReplica state change, we shrink the
//         *    isr to remove OAR - RAR in zookeeper and sent a LeaderAndIsr ONLY to the Leader to notify it of the shrunk isr.
//         *    After that, we send a StopReplica (delete = false) to the replicas in OAR - RAR.
//         * 9. Move all replicas in OAR - RAR to NonExistentReplica state. This will send a StopReplica (delete = false) to
//         *    the replicas in OAR - RAR to physically delete the replicas on disk.
//         * 10. Update AR in ZK with RAR.
//         * 11. Update the /admin/reassign_partitions path in ZK to remove this partition.
//         * 12. After electing leader, the replicas and isr information changes. So resend the update metadata request to every broker.
//         *
//         * For example, if OAR = {1, 2, 3} and RAR = {4,5,6}, the values in the assigned replica (AR) and leader/isr path in ZK
//         * may go through the following transition.
//         * AR                 leader/isr
//         * {1,2,3}            1/{1,2,3}           (initial state)
//         * {1,2,3,4,5,6}      1/{1,2,3}           (step 2)
//         * {1,2,3,4,5,6}      1/{1,2,3,4,5,6}     (step 4)
//         * {1,2,3,4,5,6}      4/{1,2,3,4,5,6}     (step 7)
//         * {1,2,3,4,5,6}      4/{4,5,6}           (step 8)
//         * {4,5,6}            4/{4,5,6}           (step 10)
//         *
//         * Note that we have to update AR in ZK with RAR last since it's the only place where we store OAR persistently.
//         * This way, if the controller crashes before that step, we can still recover.
//         */
//       public void onPartitionReassignment(TopicAndPartition topicAndPartition, ReassignedPartitionsContext reassignedPartitionContext) {
//            val reassignedReplicas = reassignedPartitionContext.newReplicas;
//            areReplicasInIsr(topicAndPartition.topic, topicAndPartition.partition, reassignedReplicas) match {
//                case false =>
//                    info(String.format("New replicas %s for partition %s being ",reassignedReplicas.mkString(","), topicAndPartition) +
//                            "reassigned not yet caught up with the leader");
//                    val newReplicasNotInOldReplicaList = reassignedReplicas.toSet -- controllerContext.partitionReplicaAssignment(topicAndPartition).toSet;
//                    val newAndOldReplicas = (reassignedPartitionContext.newReplicas ++ controllerContext.partitionReplicaAssignment(topicAndPartition)).toSet;
//                    //1. Update AR in ZK with OAR + RAR.;
//                    updateAssignedReplicasForPartition(topicAndPartition, newAndOldReplicas.toSeq);
//                    //2. Send LeaderAndIsr request to every replica in OAR + RAR (with AR as OAR + RAR).;
//                    updateLeaderEpochAndSendRequest(topicAndPartition, controllerContext.partitionReplicaAssignment(topicAndPartition),
//                            newAndOldReplicas.toSeq);
//                    //3. replicas in RAR - OAR -> NewReplica;
//                    startNewReplicasForReassignedPartition(topicAndPartition, reassignedPartitionContext, newReplicasNotInOldReplicaList);
//                    info(String.format("Waiting for new replicas %s for partition %s being ",reassignedReplicas.mkString(","), topicAndPartition) +
//                            "reassigned to catch up with the leader");
//                case true =>
//                    //4. Wait until all replicas in RAR are in sync with the leader.;
//                    val oldReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition).toSet -- reassignedReplicas.toSet;
//                    //5. replicas in RAR -> OnlineReplica;
//                    reassignedReplicas.foreach { replica =>
//                    replicaStateMachine.handleStateChanges(Set(new PartitionAndReplica(topicAndPartition.topic, topicAndPartition.partition,
//                            replica)), OnlineReplica);
//                }
//                //6. Set AR to RAR in memory.;
//                //7. Send LeaderAndIsr request with a potential new leader (if current leader not in RAR) and;
//                //   a new AR (using RAR) and same isr to every broker in RAR;
//                moveReassignedPartitionLeaderIfRequired(topicAndPartition, reassignedPartitionContext);
//                //8. replicas in OAR - RAR -> Offline (force those replicas out of isr)
//                //9. replicas in OAR - RAR -> NonExistentReplica (force those replicas to be deleted)
//                stopOldReplicasOfReassignedPartition(topicAndPartition, reassignedPartitionContext, oldReplicas);
//                //10. Update AR in ZK with RAR.;
//                updateAssignedReplicasForPartition(topicAndPartition, reassignedReplicas);
//                //11. Update the /admin/reassign_partitions path in ZK to remove this partition.;
//                removePartitionFromReassignedPartitions(topicAndPartition);
//                info(String.format("Removed partition %s from the list of reassigned partitions in zookeeper",topicAndPartition))
//                controllerContext.partitionsBeingReassigned.remove(topicAndPartition);
//                //12. After electing leader, the replicas and isr information changes, so resend the update metadata request to every broker;
//                sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq, Set(topicAndPartition));
//                // signal delete topic thread if reassignment for some partitions belonging to topics being deleted just completed;
//                deleteTopicManager.resumeDeletionForTopics(Set(topicAndPartition.topic));
//            }
//        }
//
//    privatepublic void watchIsrChangesForReassignedPartition(String topic,
//                                                      Int partition,
//                                                      ReassignedPartitionsContext reassignedPartitionContext) {
//        val reassignedReplicas = reassignedPartitionContext.newReplicas;
//        val isrChangeListener = new ReassignedPartitionsIsrChangeListener(this, topic, partition,
//                reassignedReplicas.toSet);
//        reassignedPartitionContext.isrChangeListener = isrChangeListener;
//        // register listener on the leader and isr path to wait until they catch up with the current leader;
//        zkClient.subscribeDataChanges(ZkUtils.getTopicPartitionLeaderAndIsrPath(topic, partition), isrChangeListener);
//    }
//
//   public void initiateReassignReplicasForTopicPartition(TopicAndPartition topicAndPartition,
//                                                  ReassignedPartitionsContext reassignedPartitionContext) {
//        val newReplicas = reassignedPartitionContext.newReplicas;
//        val topic = topicAndPartition.topic;
//        val partition = topicAndPartition.partition;
//        val aliveNewReplicas = newReplicas.filter(r => controllerContext.liveBrokerIds.contains(r));
//        try {
//            val assignedReplicasOpt = controllerContext.partitionReplicaAssignment.get(topicAndPartition);
//            assignedReplicasOpt match {
//                case Some(assignedReplicas) =>
//                    if(assignedReplicas == newReplicas) {
//                        throw new KafkaException(String.format("Partition %s to be reassigned is already assigned to replicas",topicAndPartition) +
//                                String.format(" %s. Ignoring request for partition reassignment",newReplicas.mkString(",")))
//                    } else {
//                        if(aliveNewReplicas == newReplicas) {
//                            info(String.format("Handling reassignment of partition %s to new replicas %s",topicAndPartition, newReplicas.mkString(",")))
//                            // first register ISR change listener;
//                            watchIsrChangesForReassignedPartition(topic, partition, reassignedPartitionContext);
//                            controllerContext.partitionsBeingReassigned.put(topicAndPartition, reassignedPartitionContext);
//                            // mark topic ineligible for deletion for the partitions being reassigned;
//                            deleteTopicManager.markTopicIneligibleForDeletion(Set(topic));
//                            onPartitionReassignment(topicAndPartition, reassignedPartitionContext);
//                        } else {
//                            // some replica in RAR is not alive. Fail partition reassignment;
//                            throw new KafkaException(String.format("Only %s replicas out of the new set of replicas",aliveNewReplicas.mkString(",")) +
//                                    String.format(" %s for partition %s to be reassigned are alive. ",newReplicas.mkString(","), topicAndPartition) +
//                                    "Failing partition reassignment");
//                        }
//                    }
//                case None => throw new KafkaException("Attempt to reassign partition %s that doesn't exist";
//                        .format(topicAndPartition))
//            }
//        } catch {
//            case Throwable e => error(String.format("Error completing reassignment of partition %s",topicAndPartition), e)
//                // remove the partition from the admin path to unblock the admin client;
//                removePartitionFromReassignedPartitions(topicAndPartition);
//        }
//    }
//
//   public void onPreferredReplicaElection(Set partitions<TopicAndPartition>, Boolean isTriggeredByAutoRebalance = false) {
//        info(String.format("Starting preferred replica leader election for partitions %s",partitions.mkString(",")))
//        try {
//            controllerContext.partitionsUndergoingPreferredReplicaElection ++= partitions;
//            deleteTopicManager.markTopicIneligibleForDeletion(partitions.map(_.topic));
//            partitionStateMachine.handleStateChanges(partitions, OnlinePartition, preferredReplicaPartitionLeaderSelector);
//        } catch {
//            case Throwable e => error(String.format("Error completing preferred replica leader election for partitions %s",partitions.mkString(",")), e)
//        } finally {
//            removePartitionsFromPreferredReplicaElection(partitions, isTriggeredByAutoRebalance);
//            deleteTopicManager.resumeDeletionForTopics(partitions.map(_.topic));
//        }
//    }
//
//    /**
//     * Invoked when the controller module of a Kafka server is started up. This does not assume that the current broker
//     * is the controller. It merely registers the session expiration listener and starts the controller leader
//     * elector
//     */
//   public void startup() = {
//        inLock(controllerContext.controllerLock) {
//            info("Controller starting up");
//            registerSessionExpirationListener();
//            isRunning = true;
//            controllerElector.startup;
//            info("Controller startup complete");
//        }
//    }
//
//    /**
//     * Invoked when the controller module of a Kafka server is shutting down. If the broker was the current controller,
//     * it shuts down the partition and replica state machines. If not, those are a no-op. In addition to that, it also
//     * shuts down the controller channel manager, if one exists (i.e. if it was the current controller)
//     */
//   public void shutdown() = {
//        inLock(controllerContext.controllerLock) {
//            isRunning = false;
//        }
//        onControllerResignation();
//    }
//
//   public void sendRequest(brokerId : Int, request : RequestOrResponse, callback: (RequestOrResponse) => Unit = null) = {
//        controllerContext.controllerChannelManager.sendRequest(brokerId, request, callback);
//    }
//
//   public void incrementControllerEpoch(ZkClient zkClient) = {
//        try {
//            var newControllerEpoch = controllerContext.epoch + 1;
//            val (updateSucceeded, newVersion) = ZkUtils.conditionalUpdatePersistentPathIfExists(zkClient,
//                    ZkUtils.ControllerEpochPath, newControllerEpoch.toString, controllerContext.epochZkVersion);
//            if(!updateSucceeded)
//                throw new ControllerMovedException("Controller moved to another broker. Aborting controller startup procedure");
//            else {
//                controllerContext.epochZkVersion = newVersion;
//                controllerContext.epoch = newControllerEpoch;
//            }
//        } catch {
//            case ZkNoNodeException nne =>
//                // if path doesn't exist, this is the first controller whose epoch should be 1;
//                // the following call can still fail if another controller gets elected between checking if the path exists and;
//                // trying to create the controller epoch path;
//                try {
//                    zkClient.createPersistent(ZkUtils.ControllerEpochPath, KafkaController.InitialControllerEpoch.toString);
//                    controllerContext.epoch = KafkaController.InitialControllerEpoch;
//                    controllerContext.epochZkVersion = KafkaController.InitialControllerEpochZkVersion;
//                } catch {
//                case ZkNodeExistsException e => throw new ControllerMovedException("Controller moved to another broker. " +
//                        "Aborting controller startup procedure");
//                case Throwable oe => error("Error while incrementing controller epoch", oe);
//            }
//            case Throwable oe => error("Error while incrementing controller epoch", oe);
//
//        }
//        info(String.format("Controller %d incremented epoch to %d",config.brokerId, controllerContext.epoch))
//    }
//
//    privatepublic void registerSessionExpirationListener() = {
//        zkClient.subscribeStateChanges(new SessionExpirationListener());
//    }
//
//    privatepublic void initializeControllerContext() {
//        // update controller cache with delete topic information;
//        controllerContext.liveBrokers = ZkUtils.getAllBrokersInCluster(zkClient).toSet;
//        controllerContext.allTopics = ZkUtils.getAllTopics(zkClient).toSet;
//        controllerContext.partitionReplicaAssignment = ZkUtils.getReplicaAssignmentForTopics(zkClient, controllerContext.allTopics.toSeq);
//        controllerContext.partitionLeadershipInfo = new mutable.HashMap<TopicAndPartition, LeaderIsrAndControllerEpoch>
//        controllerContext.shuttingDownBrokerIds = mutable.Set.empty<Integer>
//        // update the leader and isr cache for all existing partitions from Zookeeper;
//        updateLeaderAndIsrCache();
//        // start the channel manager;
//        startChannelManager();
//        initializePreferredReplicaElection();
//        initializePartitionReassignment();
//        initializeTopicDeletion();
//        info(String.format("Currently active brokers in the cluster: %s",controllerContext.liveBrokerIds))
//        info(String.format("Currently shutting brokers in the cluster: %s",controllerContext.shuttingDownBrokerIds))
//        info(String.format("Current list of topics in the cluster: %s",controllerContext.allTopics))
//    }
//
//    privatepublic void initializePreferredReplicaElection() {
//        // initialize preferred replica election state;
//        val partitionsUndergoingPreferredReplicaElection = ZkUtils.getPartitionsUndergoingPreferredReplicaElection(zkClient);
//        // check if they are already completed or topic was deleted;
//        val partitionsThatCompletedPreferredReplicaElection = partitionsUndergoingPreferredReplicaElection.filter { partition =>
//            val replicasOpt = controllerContext.partitionReplicaAssignment.get(partition);
//            val topicDeleted = replicasOpt.isEmpty;
//            val successful =
//            if(!topicDeleted) controllerContext.partitionLeadershipInfo(partition).leaderAndIsr.leader == replicasOpt.get.head else false;
//            successful || topicDeleted;
//        }
//        controllerContext.partitionsUndergoingPreferredReplicaElection ++= partitionsUndergoingPreferredReplicaElection;
//        controllerContext.partitionsUndergoingPreferredReplicaElection --= partitionsThatCompletedPreferredReplicaElection;
//        info(String.format("Partitions undergoing preferred replica election: %s",partitionsUndergoingPreferredReplicaElection.mkString(",")))
//        info(String.format("Partitions that completed preferred replica election: %s",partitionsThatCompletedPreferredReplicaElection.mkString(",")))
//        info(String.format("Resuming preferred replica election for partitions: %s",controllerContext.partitionsUndergoingPreferredReplicaElection.mkString(",")))
//    }
//
//    privatepublic void initializePartitionReassignment() {
//        // read the partitions being reassigned from zookeeper path /admin/reassign_partitions;
//        val partitionsBeingReassigned = ZkUtils.getPartitionsBeingReassigned(zkClient);
//        // check if they are already completed or topic was deleted;
//        val reassignedPartitions = partitionsBeingReassigned.filter { partition =>
//            val replicasOpt = controllerContext.partitionReplicaAssignment.get(partition._1);
//            val topicDeleted = replicasOpt.isEmpty;
//            val successful = if(!topicDeleted) replicasOpt.get == partition._2.newReplicas else false;
//            topicDeleted || successful;
//        }.map(_._1);
//        reassignedPartitions.foreach(p => removePartitionFromReassignedPartitions(p))
//        var mutable partitionsToReassign.Map<TopicAndPartition, ReassignedPartitionsContext> = new mutable.HashMap;
//        partitionsToReassign ++= partitionsBeingReassigned;
//        partitionsToReassign --= reassignedPartitions;
//        controllerContext.partitionsBeingReassigned ++= partitionsToReassign;
//        info(String.format("Partitions being reassigned: %s",partitionsBeingReassigned.toString()))
//        info(String.format("Partitions already reassigned: %s",reassignedPartitions.toString()))
//        info(String.format("Resuming reassignment of partitions: %s",partitionsToReassign.toString()))
//    }
//
//    privatepublic void initializeTopicDeletion() {
//        val topicsQueuedForDeletion = ZkUtils.getChildrenParentMayNotExist(zkClient, ZkUtils.DeleteTopicsPath).toSet;
//        val topicsWithReplicasOnDeadBrokers = controllerContext.partitionReplicaAssignment.filter { case(partition, replicas) =>
//            replicas.exists(r => !controllerContext.liveBrokerIds.contains(r)) }.keySet.map(_.topic);
//        val topicsForWhichPartitionReassignmentIsInProgress = controllerContext.partitionsUndergoingPreferredReplicaElection.map(_.topic);
//        val topicsForWhichPreferredReplicaElectionIsInProgress = controllerContext.partitionsBeingReassigned.keySet.map(_.topic);
//        val topicsIneligibleForDeletion = topicsWithReplicasOnDeadBrokers | topicsForWhichPartitionReassignmentIsInProgress |;
//                topicsForWhichPreferredReplicaElectionIsInProgress;
//        info(String.format("List of topics to be deleted: %s",topicsQueuedForDeletion.mkString(",")))
//        info(String.format("List of topics ineligible for deletion: %s",topicsIneligibleForDeletion.mkString(",")))
//        // initialize the topic deletion manager;
//        deleteTopicManager = new TopicDeletionManager(this, topicsQueuedForDeletion, topicsIneligibleForDeletion);
//    }
//
//    privatepublic void maybeTriggerPartitionReassignment() {
//        controllerContext.partitionsBeingReassigned.foreach { topicPartitionToReassign =>
//            initiateReassignReplicasForTopicPartition(topicPartitionToReassign._1, topicPartitionToReassign._2);
//        }
//    }
//
//    privatepublic void maybeTriggerPreferredReplicaElection() {
//        onPreferredReplicaElection(controllerContext.partitionsUndergoingPreferredReplicaElection.toSet);
//    }
//
//    privatepublic void startChannelManager() {
//        controllerContext.controllerChannelManager = new ControllerChannelManager(controllerContext, config);
//        controllerContext.controllerChannelManager.startup();
//    }
//
//    privatepublic void updateLeaderAndIsrCache() {
//        val leaderAndIsrInfo = ZkUtils.getPartitionLeaderAndIsrForTopics(zkClient, controllerContext.partitionReplicaAssignment.keySet);
//        for((topicPartition, leaderIsrAndControllerEpoch) <- leaderAndIsrInfo)
//        controllerContext.partitionLeadershipInfo.put(topicPartition, leaderIsrAndControllerEpoch);
//    }
//
//    privatepublic Boolean  void areReplicasInIsr(String topic, Int partition, Seq replicas<Integer>) {
//        getLeaderAndIsrForPartition(zkClient, topic, partition) match {
//            case Some(leaderAndIsr) =>
//                val replicasNotInIsr = replicas.filterNot(r => leaderAndIsr.isr.contains(r));
//                replicasNotInIsr.isEmpty;
//            case None => false;
//        }
//    }
//
//    privatepublic void moveReassignedPartitionLeaderIfRequired(TopicAndPartition topicAndPartition,
//                                                        ReassignedPartitionsContext reassignedPartitionContext) {
//        val reassignedReplicas = reassignedPartitionContext.newReplicas;
//        val currentLeader = controllerContext.partitionLeadershipInfo(topicAndPartition).leaderAndIsr.leader;
//        // change the assigned replica list to just the reassigned replicas in the cache so it gets sent out on the LeaderAndIsr;
//        // request to the current or new leader. This will prevent it from adding the old replicas to the ISR;
//        val oldAndNewReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition);
//        controllerContext.partitionReplicaAssignment.put(topicAndPartition, reassignedReplicas);
//        if(!reassignedPartitionContext.newReplicas.contains(currentLeader)) {
//            info(String.format("Leader %s for partition %s being reassigned, ",currentLeader, topicAndPartition) +
//                    String.format("is not in the new list of replicas %s. Re-electing leader",reassignedReplicas.mkString(",")))
//            // move the leader to one of the alive and caught up new replicas;
//            partitionStateMachine.handleStateChanges(Set(topicAndPartition), OnlinePartition, reassignedPartitionLeaderSelector);
//        } else {
//            // check if the leader is alive or not;
//            controllerContext.liveBrokerIds.contains(currentLeader) match {
//                case true =>
//                    info(String.format("Leader %s for partition %s being reassigned, ",currentLeader, topicAndPartition) +
//                            String.format("is already in the new list of replicas %s and is alive",reassignedReplicas.mkString(",")))
//                    // shrink replication factor and update the leader epoch in zookeeper to use on the next LeaderAndIsrRequest;
//                    updateLeaderEpochAndSendRequest(topicAndPartition, oldAndNewReplicas, reassignedReplicas);
//                case false =>
//                    info(String.format("Leader %s for partition %s being reassigned, ",currentLeader, topicAndPartition) +
//                            String.format("is already in the new list of replicas %s but is dead",reassignedReplicas.mkString(",")))
//                    partitionStateMachine.handleStateChanges(Set(topicAndPartition), OnlinePartition, reassignedPartitionLeaderSelector);
//            }
//        }
//    }
//
//    privatepublic void stopOldReplicasOfReassignedPartition(TopicAndPartition topicAndPartition,
//                                                     ReassignedPartitionsContext reassignedPartitionContext,
//                                                     Set oldReplicas<Integer>) {
//        val topic = topicAndPartition.topic;
//        val partition = topicAndPartition.partition;
//        // first move the replica to offline state (the controller removes it from the ISR);
//        val replicasToBeDeleted = oldReplicas.map(r => PartitionAndReplica(topic, partition, r));
//        replicaStateMachine.handleStateChanges(replicasToBeDeleted, OfflineReplica);
//        // send stop replica command to the old replicas;
//        replicaStateMachine.handleStateChanges(replicasToBeDeleted, ReplicaDeletionStarted);
//        // Eventually TODO partition reassignment could use a callback that does retries if deletion failed;
//        replicaStateMachine.handleStateChanges(replicasToBeDeleted, ReplicaDeletionSuccessful);
//        replicaStateMachine.handleStateChanges(replicasToBeDeleted, NonExistentReplica);
//    }
//
//    privatepublic void updateAssignedReplicasForPartition(TopicAndPartition topicAndPartition,
//                                                   Seq replicas<Integer>) {
//        val partitionsAndReplicasForThisTopic = controllerContext.partitionReplicaAssignment.filter(_._1.topic.equals(topicAndPartition.topic));
//        partitionsAndReplicasForThisTopic.put(topicAndPartition, replicas);
//        updateAssignedReplicasForPartition(topicAndPartition, partitionsAndReplicasForThisTopic);
//        info(String.format("Updated assigned replicas for partition %s being reassigned to %s ",topicAndPartition, replicas.mkString(",")))
//        // update the assigned replica list after a successful zookeeper write;
//        controllerContext.partitionReplicaAssignment.put(topicAndPartition, replicas);
//    }
//
//    privatepublic void startNewReplicasForReassignedPartition(TopicAndPartition topicAndPartition,
//                                                       ReassignedPartitionsContext reassignedPartitionContext,
//                                                       Set newReplicas<Integer>) {
//        // send the start replica request to the brokers in the reassigned replicas list that are not in the assigned;
//        // replicas list;
//        newReplicas.foreach { replica =>
//            replicaStateMachine.handleStateChanges(Set(new PartitionAndReplica(topicAndPartition.topic, topicAndPartition.partition, replica)), NewReplica);
//        }
//    }
//
//    privatepublic void updateLeaderEpochAndSendRequest(TopicAndPartition topicAndPartition, Seq replicasToReceiveRequest<Int], Seq newAssignedReplicas[Integer>) {
//        brokerRequestBatch.newBatch();
//        updateLeaderEpoch(topicAndPartition.topic, topicAndPartition.partition) match {
//            case Some(updatedLeaderIsrAndControllerEpoch) =>
//                brokerRequestBatch.addLeaderAndIsrRequestForBrokers(replicasToReceiveRequest, topicAndPartition.topic,
//                        topicAndPartition.partition, updatedLeaderIsrAndControllerEpoch, newAssignedReplicas);
//                brokerRequestBatch.sendRequestsToBrokers(controllerContext.epoch, controllerContext.correlationId.getAndIncrement);
//                stateChangeLogger.trace(("Controller %d epoch %d sent LeaderAndIsr request %s with new assigned replica list %s " +
//                        "to leader %d for partition being reassigned %s").format(config.brokerId, controllerContext.epoch, updatedLeaderIsrAndControllerEpoch,
//                        newAssignedReplicas.mkString(","), updatedLeaderIsrAndControllerEpoch.leaderAndIsr.leader, topicAndPartition));
//            case None => // fail the reassignment;
//                stateChangeLogger.error(("Controller %d epoch %d failed to send LeaderAndIsr request with new assigned replica list %s " +
//                        "to leader for partition being reassigned %s").format(config.brokerId, controllerContext.epoch,
//                        newAssignedReplicas.mkString(","), topicAndPartition));
//        }
//    }
//
//    privatepublic void registerReassignedPartitionsListener() = {
//        zkClient.subscribeDataChanges(ZkUtils.ReassignPartitionsPath, partitionReassignedListener);
//    }
//
//    privatepublic void deregisterReassignedPartitionsListener() = {
//        zkClient.unsubscribeDataChanges(ZkUtils.ReassignPartitionsPath, partitionReassignedListener);
//    }
//
//    privatepublic void registerPreferredReplicaElectionListener() {
//        zkClient.subscribeDataChanges(ZkUtils.PreferredReplicaLeaderElectionPath, preferredReplicaElectionListener);
//    }
//
//    privatepublic void deregisterPreferredReplicaElectionListener() {
//        zkClient.unsubscribeDataChanges(ZkUtils.PreferredReplicaLeaderElectionPath, preferredReplicaElectionListener);
//    }
//
//    privatepublic void deregisterReassignedPartitionsIsrChangeListeners() {
//        controllerContext.partitionsBeingReassigned.foreach {
//            case (topicAndPartition, reassignedPartitionsContext) =>
//                val zkPartitionPath = ZkUtils.getTopicPartitionLeaderAndIsrPath(topicAndPartition.topic, topicAndPartition.partition);
//                zkClient.unsubscribeDataChanges(zkPartitionPath, reassignedPartitionsContext.isrChangeListener);
//        }
//    }
//
//    privatepublic void readControllerEpochFromZookeeper() {
//        // initialize the controller epoch and zk version by reading from zookeeper;
//        if(ZkUtils.pathExists(controllerContext.zkClient, ZkUtils.ControllerEpochPath)) {
//            val epochData = ZkUtils.readData(controllerContext.zkClient, ZkUtils.ControllerEpochPath);
//            controllerContext.epoch = epochData._1.toInt;
//            controllerContext.epochZkVersion = epochData._2.getVersion;
//            info(String.format("Initialized controller epoch to %d and zk version %d",controllerContext.epoch, controllerContext.epochZkVersion))
//        }
//    }
//
//   public void removePartitionFromReassignedPartitions(TopicAndPartition topicAndPartition) {
//        if(controllerContext.partitionsBeingReassigned.get(topicAndPartition).isDefined) {
//            // stop watching the ISR changes for this partition;
//            zkClient.unsubscribeDataChanges(ZkUtils.getTopicPartitionLeaderAndIsrPath(topicAndPartition.topic, topicAndPartition.partition),
//                    controllerContext.partitionsBeingReassigned(topicAndPartition).isrChangeListener);
//        }
//        // read the current list of reassigned partitions from zookeeper;
//        val partitionsBeingReassigned = ZkUtils.getPartitionsBeingReassigned(zkClient);
//        // remove this partition from that list;
//        val updatedPartitionsBeingReassigned = partitionsBeingReassigned - topicAndPartition;
//        // write the new list to zookeeper;
//        ZkUtils.updatePartitionReassignmentData(zkClient, updatedPartitionsBeingReassigned.mapValues(_.newReplicas));
//        // update the cache. NO-OP if the partition's reassignment was never started;
//        controllerContext.partitionsBeingReassigned.remove(topicAndPartition);
//    }
//
//   public void updateAssignedReplicasForPartition(TopicAndPartition topicAndPartition,
//                                           Map newReplicaAssignmentForTopic<TopicAndPartition, Seq<Int>>) {
//        try {
//            val zkPath = ZkUtils.getTopicPath(topicAndPartition.topic);
//            val jsonPartitionMap = ZkUtils.replicaAssignmentZkData(newReplicaAssignmentForTopic.map(e => (e._1.partition.toString -> e._2)));
//            ZkUtils.updatePersistentPath(zkClient, zkPath, jsonPartitionMap);
//            debug(String.format("Updated path %s with %s for replica assignment",zkPath, jsonPartitionMap))
//        } catch {
//            case ZkNoNodeException e => throw new IllegalStateException(String.format("Topic %s doesn't exist",topicAndPartition.topic))
//            case Throwable e2 => throw new KafkaException(e2.toString);
//        }
//    }
//
//   public void removePartitionsFromPreferredReplicaElection(Set partitionsToBeRemoved<TopicAndPartition>,
//                                                     isTriggeredByAutoRebalance : Boolean) {
//        for(partition <- partitionsToBeRemoved) {
//            // check the status;
//            val currentLeader = controllerContext.partitionLeadershipInfo(partition).leaderAndIsr.leader;
//            val preferredReplica = controllerContext.partitionReplicaAssignment(partition).head;
//            if(currentLeader == preferredReplica) {
//                info(String.format("Partition %s completed preferred replica leader election. New leader is %d",partition, preferredReplica))
//            } else {
//                warn(String.format("Partition %s failed to complete preferred replica leader election. Leader is %d",partition, currentLeader))
//            }
//        }
//        if (!isTriggeredByAutoRebalance)
//            ZkUtils.deletePath(zkClient, ZkUtils.PreferredReplicaLeaderElectionPath);
//        controllerContext.partitionsUndergoingPreferredReplicaElection --= partitionsToBeRemoved;
//    }
//
//    /**
//     * Send the leader information for selected partitions to selected brokers so that they can correctly respond to
//     * metadata requests
//     * @param brokers The brokers that the update metadata request should be sent to
//     */
//   public void sendUpdateMetadataRequest(Seq brokers<Int], Set partitions<TopicAndPartition> = Set.empty[TopicAndPartition>) {
//        brokerRequestBatch.newBatch();
//        brokerRequestBatch.addUpdateMetadataRequestForBrokers(brokers, partitions);
//        brokerRequestBatch.sendRequestsToBrokers(epoch, controllerContext.correlationId.getAndIncrement);
//    }
//
//    /**
//     * Removes a given partition replica from the ISR; if it is not the current
//     * leader and there are sufficient remaining replicas in ISR.
//     * @param topic topic
//     * @param partition partition
//     * @param replicaId replica Id
//     * @return the new leaderAndIsr (with the replica removed if it was present),
//     *         or None if leaderAndIsr is empty.
//     */
//   public void removeReplicaFromIsr(String topic, Int partition, Int replicaId): Option<LeaderIsrAndControllerEpoch> = {
//        val topicAndPartition = TopicAndPartition(topic, partition);
//        debug(String.format("Removing replica %d from ISR %s for partition %s.",replicaId,
//                controllerContext.partitionLeadershipInfo(topicAndPartition).leaderAndIsr.isr.mkString(","), topicAndPartition));
//        var Option finalLeaderIsrAndControllerEpoch<LeaderIsrAndControllerEpoch> = None;
//        var zkWriteCompleteOrUnnecessary = false;
//        while (!zkWriteCompleteOrUnnecessary) {
//            // refresh leader and isr from zookeeper again;
//            val leaderIsrAndEpochOpt = ReplicationUtils.getLeaderIsrAndEpochForPartition(zkClient, topic, partition);
//            zkWriteCompleteOrUnnecessary = leaderIsrAndEpochOpt match {
//                case Some(leaderIsrAndEpoch) => // increment the leader epoch even if the ISR changes;
//                    val leaderAndIsr = leaderIsrAndEpoch.leaderAndIsr;
//                    val controllerEpoch = leaderIsrAndEpoch.controllerEpoch;
//                    if(controllerEpoch > epoch)
//                        throw new StateChangeFailedException("Leader and isr path written by another controller. This probably" +
//                                String.format("means the current controller with epoch %d went through a soft failure and another ",epoch) +
//                                String.format("controller was elected with epoch %d. Aborting state change by this controller",controllerEpoch))
//                    if (leaderAndIsr.isr.contains(replicaId)) {
//                        // if the replica to be removed from the ISR is also the leader, set the new leader value to -1;
//                        val newLeader = if (replicaId == leaderAndIsr.leader) LeaderAndIsr.NoLeader else leaderAndIsr.leader;
//                        var newIsr = leaderAndIsr.isr.filter(b => b != replicaId);
//
//                        // if the replica to be removed from the ISR is the last surviving member of the ISR and unclean leader election;
//                        // is disallowed for the corresponding topic, then we must preserve the ISR membership so that the replica can;
//                        // eventually be restored as the leader.;
//                        if (newIsr.isEmpty && !LogConfig.fromProps(config.props.props, AdminUtils.fetchTopicConfig(zkClient,
//                                topicAndPartition.topic)).uncleanLeaderElectionEnable) {
//                            info(String.format("Retaining last ISR %d of partition %s since unclean leader election is disabled",replicaId, topicAndPartition))
//                            newIsr = leaderAndIsr.isr;
//                        }
//
//                        val newLeaderAndIsr = new LeaderAndIsr(newLeader, leaderAndIsr.leaderEpoch + 1,
//                                newIsr, leaderAndIsr.zkVersion + 1);
//                        // update the new leadership decision in zookeeper or retry;
//                        val (updateSucceeded, newVersion) = ReplicationUtils.updateLeaderAndIsr(zkClient, topic, partition,
//                                newLeaderAndIsr, epoch, leaderAndIsr.zkVersion);
//
//                        newLeaderAndIsr.zkVersion = newVersion;
//                        finalLeaderIsrAndControllerEpoch = Some(LeaderIsrAndControllerEpoch(newLeaderAndIsr, epoch));
//                        controllerContext.partitionLeadershipInfo.put(topicAndPartition, finalLeaderIsrAndControllerEpoch.get);
//                        if (updateSucceeded)
//                            info(String.format("New leader and ISR for partition %s is %s",topicAndPartition, newLeaderAndIsr.toString()))
//                        updateSucceeded;
//                    } else {
//                        warn("Cannot remove replica %d from ISR of partition %s since it is not in the ISR. Leader = %d ; ISR = %s";
//                                .format(replicaId, topicAndPartition, leaderAndIsr.leader, leaderAndIsr.isr))
//                        finalLeaderIsrAndControllerEpoch = Some(LeaderIsrAndControllerEpoch(leaderAndIsr, epoch));
//                        controllerContext.partitionLeadershipInfo.put(topicAndPartition, finalLeaderIsrAndControllerEpoch.get);
//                        true;
//                    }
//                case None =>
//                    warn(String.format("Cannot remove replica %d from ISR of %s - leaderAndIsr is empty.",replicaId, topicAndPartition))
//                    true;
//            }
//        }
//        finalLeaderIsrAndControllerEpoch;
//    }
//
//    /**
//     * Does not change leader or isr, but just increments the leader epoch
//     * @param topic topic
//     * @param partition partition
//     * @return the new leaderAndIsr with an incremented leader epoch, or None if leaderAndIsr is empty.
//     */
//    privatepublic void updateLeaderEpoch(String topic, Int partition): Option<LeaderIsrAndControllerEpoch> = {
//        val topicAndPartition = TopicAndPartition(topic, partition);
//        debug(String.format("Updating leader epoch for partition %s.",topicAndPartition))
//        var Option finalLeaderIsrAndControllerEpoch<LeaderIsrAndControllerEpoch> = None;
//        var zkWriteCompleteOrUnnecessary = false;
//        while (!zkWriteCompleteOrUnnecessary) {
//            // refresh leader and isr from zookeeper again;
//            val leaderIsrAndEpochOpt = ReplicationUtils.getLeaderIsrAndEpochForPartition(zkClient, topic, partition);
//            zkWriteCompleteOrUnnecessary = leaderIsrAndEpochOpt match {
//                case Some(leaderIsrAndEpoch) =>
//                    val leaderAndIsr = leaderIsrAndEpoch.leaderAndIsr;
//                    val controllerEpoch = leaderIsrAndEpoch.controllerEpoch;
//                    if(controllerEpoch > epoch)
//                        throw new StateChangeFailedException("Leader and isr path written by another controller. This probably" +
//                                String.format("means the current controller with epoch %d went through a soft failure and another ",epoch) +
//                                String.format("controller was elected with epoch %d. Aborting state change by this controller",controllerEpoch))
//                    // increment the leader epoch even if there are no leader or isr changes to allow the leader to cache the expanded;
//                    // assigned replica list;
//                    val newLeaderAndIsr = new LeaderAndIsr(leaderAndIsr.leader, leaderAndIsr.leaderEpoch + 1,
//                            leaderAndIsr.isr, leaderAndIsr.zkVersion + 1);
//                    // update the new leadership decision in zookeeper or retry;
//                    val (updateSucceeded, newVersion) = ReplicationUtils.updateLeaderAndIsr(zkClient, topic,
//                            partition, newLeaderAndIsr, epoch, leaderAndIsr.zkVersion);
//
//                    newLeaderAndIsr.zkVersion = newVersion;
//                    finalLeaderIsrAndControllerEpoch = Some(LeaderIsrAndControllerEpoch(newLeaderAndIsr, epoch));
//                    if (updateSucceeded)
//                        info(String.format("Updated leader epoch for partition %s to %d",topicAndPartition, newLeaderAndIsr.leaderEpoch))
//                    updateSucceeded;
//                case None =>
//                    throw new IllegalStateException(("Cannot update leader epoch for partition %s as leaderAndIsr path is empty. " +
//                            "This could mean we somehow tried to reassign a partition that doesn't exist").format(topicAndPartition))
//                    true;
//            }
//        }
//        finalLeaderIsrAndControllerEpoch;
//    }
//
//    class SessionExpirationListener() extends IZkStateListener with Logging {
//        this.logIdent = "<SessionExpirationListener on " + config.brokerId + ">, ";
//    @throws(classOf<Exception>)
//       public void handleStateChanged(KeeperState state) {
//            // do nothing, since zkclient will do reconnect for us.;
//        }
//
//        /**
//         * Called after the zookeeper session has expired and a new session has been created. You would have to re-create
//         * any ephemeral nodes here.
//         *
//         * @throws Exception
//         *             On any error.
//         */
//    @throws(classOf<Exception>)
//       public void handleNewSession() {
//            info("ZK expired; shut down all controller components and try to re-elect");
//            inLock(controllerContext.controllerLock) {
//                onControllerResignation();
//                controllerElector.elect;
//            }
//        }
//    }
//
//    privatepublic Unit  void checkAndTriggerPartitionRebalance() {
//        if (isActive()) {
//            trace("checking need to trigger partition rebalance");
//            // get all the active brokers;
//            var Map preferredReplicasForTopicsByBrokers<Integer,> Map<TopicAndPartition, Seq[Int]>> = null;
//            inLock(controllerContext.controllerLock) {
//                preferredReplicasForTopicsByBrokers =
//                        controllerContext.partitionReplicaAssignment.filterNot(p => deleteTopicManager.isTopicQueuedUpForDeletion(p._1.topic)).groupBy {
//                    case(topicAndPartition, assignedReplicas) => assignedReplicas.head;
//                }
//            }
//            debug("preferred replicas by broker " + preferredReplicasForTopicsByBrokers);
//            // for each broker, check if a preferred replica election needs to be triggered;
//            preferredReplicasForTopicsByBrokers.foreach {
//                case(leaderBroker, topicAndPartitionsForBroker) => {
//                    var Double imbalanceRatio = 0;
//                    var Map topicsNotInPreferredReplica<TopicAndPartition, Seq<Int>> = null;
//                    inLock(controllerContext.controllerLock) {
//                        topicsNotInPreferredReplica =
//                                topicAndPartitionsForBroker.filter {
//                            case(topicPartition, replicas) => {
//                                controllerContext.partitionLeadershipInfo.contains(topicPartition) &&;
//                                        controllerContext.partitionLeadershipInfo(topicPartition).leaderAndIsr.leader != leaderBroker;
//                            }
//                        }
//                        debug("topics not in preferred replica " + topicsNotInPreferredReplica);
//                        val totalTopicPartitionsForBroker = topicAndPartitionsForBroker.size;
//                        val totalTopicPartitionsNotLedByBroker = topicsNotInPreferredReplica.size;
//                        imbalanceRatio = totalTopicPartitionsNotLedByBroker.toDouble / totalTopicPartitionsForBroker;
//                        trace(String.format("leader imbalance ratio for broker %d is %f",leaderBroker, imbalanceRatio))
//                    }
//                    // check ratio and if greater than desired ratio, trigger a rebalance for the topic partitions;
//                    // that need to be on this broker;
//                    if (imbalanceRatio > (config.leaderImbalancePerBrokerPercentage.toDouble / 100)) {
//                        topicsNotInPreferredReplica.foreach {
//                            case(topicPartition, replicas) => {
//                                inLock(controllerContext.controllerLock) {
//                                    // do this check only if the broker is live and there are no partitions being reassigned currently;
//                                    // and preferred replica election is not in progress;
//                                    if (controllerContext.liveBrokerIds.contains(leaderBroker) &&;
//                                            controllerContext.partitionsBeingReassigned.size == 0 &&;
//                                            controllerContext.partitionsUndergoingPreferredReplicaElection.size == 0 &&;
//                                            !deleteTopicManager.isTopicQueuedUpForDeletion(topicPartition.topic) &&;
//                                            controllerContext.allTopics.contains(topicPartition.topic)) {
//                                        onPreferredReplicaElection(Set(topicPartition), true);
//                                    }
//                                }
//                            }
//                        }
//                    }
//                }
//            }
//        }
//    }
}
