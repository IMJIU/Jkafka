package kafka.server;

import kafka.cluster.Partition;
import kafka.func.Tuple;
import kafka.log.LogManager;
import kafka.metrics.KafkaMetricsGroup;
import kafka.utils.Pool;
import kafka.utils.Scheduler;
import kafka.utils.Time;
import org.I0Itec.zkclient.ZkClient;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by zhoulf on 2017/4/1.
 */


public class ReplicaManager  extends KafkaMetricsGroup {
        public static final String HighWatermarkFilename = "replication-offset-checkpoint";
//
    KafkaConfig config;
    Time time;
        ZkClient zkClient;
    Scheduler scheduler;
     LogManager logManager;
     AtomicBoolean isShuttingDown;

        public ReplicaManager(KafkaConfig config, Time time, ZkClient zkClient, Scheduler scheduler, LogManager logManager, AtomicBoolean isShuttingDown) {
                this.config = config;
                this.time = time;
                this.zkClient = zkClient;
                this.scheduler = scheduler;
                this.logManager = logManager;
                this.isShuttingDown = isShuttingDown;
        }
        //  /* epoch of the controller that last changed the leader */
//   public volatile Integer controllerEpoch = KafkaController.InitialControllerEpoch - 1;
//        private Integer localBrokerId = config.brokerId;
//        private Pool<Tuple<String,Integer>,Partition> allPartitions = new Pool<>();
//        private Object replicaStateChangeLock = new Object();
//        public ReplicaFetcherManager replicaFetcherManager = new ReplicaFetcherManager(config, this);
//        private AtomicBoolean highWatermarkCheckPointThreadStarted = new AtomicBoolean(false);
//        val highWatermarkCheckpoints = config.logDirs.map(dir => (new File(dir).getAbsolutePath, new OffsetCheckpoint(new File(dir, ReplicaManager.HighWatermarkFilename)))).toMap;
//        private boolean hwThreadInitialized = false;
//        this.logIdent = "[Replica Manager on Broker " + localBrokerId + "]: ";
//        val stateChangeLogger = KafkaController.stateChangeLogger;
//
//        var ProducerRequestPurgatory producerRequestPurgatory = null;
//        var FetchRequestPurgatory fetchRequestPurgatory = null;
//
//        newGauge(
//                "LeaderCount",
//                new Gauge<Int> {
//            public void  value = {
//                    getLeaderPartitions().size;
//            }
//        }
//  );
//        newGauge(
//                "PartitionCount",
//                new Gauge<Int> {
//            public void  value = allPartitions.size;
//        }
//  );
//        newGauge(
//                "UnderReplicatedPartitions",
//                new Gauge<Int> {
//            public void  value = underReplicatedPartitionCount();
//        }
//  );
//        val isrExpandRate = newMeter("IsrExpandsPerSec",  "expands", TimeUnit.SECONDS);
//        val isrShrinkRate = newMeter("IsrShrinksPerSec",  "shrinks", TimeUnit.SECONDS);
//
//        public Integer  void  underReplicatedPartitionCount() {
//                getLeaderPartitions().count(_.isUnderReplicated);
//        }
//
//        public void  startHighWaterMarksCheckPointThread() = {
//        if(highWatermarkCheckPointThreadStarted.compareAndSet(false, true))
//            scheduler.schedule("highwatermark-checkpoint", checkpointHighWatermarks, period = config.replicaHighWatermarkCheckpointIntervalMs, unit = TimeUnit.MILLISECONDS);
//  }
//
//        /**
//         * Initialize the replica manager with the request purgatory
//         *
//         * will TODO be removed in 0.9 where we refactor server structure
//         */
//
//        public void  initWithRequestPurgatory(ProducerRequestPurgatory producerRequestPurgatory, FetchRequestPurgatory fetchRequestPurgatory) {
//            this.producerRequestPurgatory = producerRequestPurgatory;
//            this.fetchRequestPurgatory = fetchRequestPurgatory;
//        }
//
//        /**
//         * Unblock some delayed produce requests with the request key
//         */
//        public void  unblockDelayedProduceRequests(TopicAndPartition key) {
//            val satisfied = producerRequestPurgatory.update(key);
//            debug("Request key %s unblocked %d producer requests.";
//                    .format(key, satisfied.size))
//
//            // send any newly unblocked responses;
//            satisfied.foreach(producerRequestPurgatory.respond(_))
//        }
//
//        /**
//         * Unblock some delayed fetch requests with the request key
//         */
//        public void  unblockDelayedFetchRequests(TopicAndPartition key) {
//            val satisfied = fetchRequestPurgatory.update(key);
//            debug(String.format("Request key %s unblocked %d fetch requests.",key, satisfied.size))
//
//            // send any newly unblocked responses;
//            satisfied.foreach(fetchRequestPurgatory.respond(_))
//        }
//
//        public void  startup() {
//            // start ISR expiration thread;
//            scheduler.schedule("isr-expiration", maybeShrinkIsr, period = config.replicaLagTimeMaxMs, unit = TimeUnit.MILLISECONDS);
//        }
//
//        public void  stopReplica(String topic, Integer partitionId, Boolean deletePartition): Short  = {
//                stateChangeLogger.trace(String.format("Broker %d handling stop replica (delete=%s) for partition <%s,%d>",localBrokerId,
//                        deletePartition.toString, topic, partitionId));
//                val errorCode = ErrorMapping.NoError;
//                getPartition(topic, partitionId) match {
//        case Some(partition) =>
//            if(deletePartition) {
//                val removedPartition = allPartitions.remove((topic, partitionId));
//                if (removedPartition != null)
//                    removedPartition.delete() // this will delete the local log;
//            }
//        case None =>
//            // Delete log and corresponding folders in case replica manager doesn't hold them anymore.;
//            // This could happen when topic is being deleted while broker is down and recovers.;
//            if(deletePartition) {
//                val topicAndPartition = TopicAndPartition(topic, partitionId);
//
//                if(logManager.getLog(topicAndPartition).isDefined) {
//                    logManager.deleteLog(topicAndPartition);
//                }
//            }
//            stateChangeLogger.trace("Broker %d ignoring stop replica (delete=%s) for partition <%s,%d> as replica doesn't exist on broker";
//                    .format(localBrokerId, deletePartition, topic, partitionId))
//    }
//            stateChangeLogger.trace("Broker %d finished handling stop replica (delete=%s) for partition <%s,%d>";
//                    .format(localBrokerId, deletePartition, topic, partitionId))
//            errorCode;
//  }
//
//            public void  stopReplicas(StopReplicaRequest stopReplicaRequest): (mutable.Map<TopicAndPartition, Short>, Short) = {
//            replicaStateChangeLock synchronized {
//                val responseMap = new collection.mutable.HashMap<TopicAndPartition, Short>;
//                if(stopReplicaRequest.controllerEpoch < controllerEpoch) {
//                    stateChangeLogger.warn("Broker %d received stop replica request from an old controller epoch %d.";
//                            .format(localBrokerId, stopReplicaRequest.controllerEpoch) +;
//                            " Latest known controller epoch is %d " + controllerEpoch);
//                    (responseMap, ErrorMapping.StaleControllerEpochCode);
//                } else {
//                    controllerEpoch = stopReplicaRequest.controllerEpoch;
//                    // First stop fetchers for all partitions, then stop the corresponding replicas;
//                    replicaFetcherManager.removeFetcherForPartitions(stopReplicaRequest.partitions.map(r => TopicAndPartition(r.topic, r.partition)));
//                    for(topicAndPartition <- stopReplicaRequest.partitions){
//                        val errorCode = stopReplica(topicAndPartition.topic, topicAndPartition.partition, stopReplicaRequest.deletePartitions);
//                        responseMap.put(topicAndPartition, errorCode);
//                    }
//                    (responseMap, ErrorMapping.NoError);
//                }
//            }
//        }
//
//        public Partition  void  getOrCreatePartition(String topic, Integer partitionId) {
//                var partition = allPartitions.get((topic, partitionId));
//        if (partition == null) {
//            allPartitions.putIfNotExists((topic, partitionId), new Partition(topic, partitionId, time, this));
//            partition = allPartitions.get((topic, partitionId));
//        }
//        partition;
//  }
//
//        public void  getPartition(String topic, Integer partitionId): Option<Partition> = {
//                val partition = allPartitions.get((topic, partitionId));
//        if (partition == null)
//            None;
//        else;
//            Some(partition);
//  }
//
//        public Replica  void  getReplicaOrException(String topic, Integer partition) {
//                val replicaOpt = getReplica(topic, partition);
//        if(replicaOpt.isDefined)
//            return replicaOpt.get;
//        else;
//            throw new ReplicaNotAvailableException(String.format("Replica %d is not available for partition <%s,%d>",config.brokerId, topic, partition))
//  }
//
//        public Replica  void  getLeaderReplicaIfLocal(String topic, Integer partitionId)  {
//                val partitionOpt = getPartition(topic, partitionId);
//                partitionOpt match {
//        case None =>
//            throw new UnknownTopicOrPartitionException(String.format("Partition <%s,%d> doesn't exist on %d",topic, partitionId, config.brokerId))
//        case Some(partition) =>
//            partition.leaderReplicaIfLocal match {
//            case Some(leaderReplica) => leaderReplica;
//            case None =>
//                throw new NotLeaderForPartitionException("Leader not local for partition <%s,%d> on broker %d";
//                        .format(topic, partitionId, config.brokerId))
//        }
//    }
//  }
//
//        public void  getReplica(String topic, Integer partitionId, Integer replicaId = config.brokerId): Option<Replica> =  {
//                val partitionOpt = getPartition(topic, partitionId);
//                partitionOpt match {
//        case None => None;
//        case Some(partition) => partition.getReplica(replicaId);
//    }
//  }
//
//            /**
//             * Read from all the offset details given and return a map of
//             * (topic, partition) -> PartitionData
//             */
//            public void  readMessageSets(FetchRequest fetchRequest) = {
//            val isFetchFromFollower = fetchRequest.isFromFollower;
//            fetchRequest.requestInfo.map;
//            {
//                case (TopicAndPartition(topic, partition), PartitionFetchInfo(offset, fetchSize)) =>
//                    val partitionDataAndOffsetInfo =
//                    try {
//                        val (fetchInfo, highWatermark) = readMessageSet(topic, partition, offset, fetchSize, fetchRequest.replicaId);
//                        BrokerTopicStats.getBrokerTopicStats(topic).bytesOutRate.mark(fetchInfo.messageSet.sizeInBytes);
//                        BrokerTopicStats.getBrokerAllTopicsStats.bytesOutRate.mark(fetchInfo.messageSet.sizeInBytes);
//                        if (isFetchFromFollower) {
//                            debug("Partition <%s,%d> received fetch request from follower %d";
//                                    .format(topic, partition, fetchRequest.replicaId))
//                        }
//                        new PartitionDataAndOffset(new FetchResponsePartitionData(ErrorMapping.NoError, highWatermark, fetchInfo.messageSet), fetchInfo.fetchOffset);
//                    } catch {
//                    // Failed NOTE fetch requests is not incremented for UnknownTopicOrPartitionException and NotLeaderForPartitionException;
//                    // since failed fetch requests metric is supposed to indicate failure of a broker in handling a fetch request;
//                    // for a partition it is the leader for;
//                    case UnknownTopicOrPartitionException utpe =>
//                        warn(String.format("Fetch request with correlation id %d from client %s on partition <%s,%d> failed due to %s",
//                                fetchRequest.correlationId, fetchRequest.clientId, topic, partition, utpe.getMessage));
//                        new PartitionDataAndOffset(new FetchResponsePartitionData(ErrorMapping.codeFor(utpe.getClass.asInstanceOf<Class<Throwable>>), -1L, MessageSet.Empty), LogOffsetMetadata.UnknownOffsetMetadata);
//                    case NotLeaderForPartitionException nle =>
//                        warn(String.format("Fetch request with correlation id %d from client %s on partition <%s,%d> failed due to %s",
//                                fetchRequest.correlationId, fetchRequest.clientId, topic, partition, nle.getMessage));
//                        new PartitionDataAndOffset(new FetchResponsePartitionData(ErrorMapping.codeFor(nle.getClass.asInstanceOf<Class<Throwable>>), -1L, MessageSet.Empty), LogOffsetMetadata.UnknownOffsetMetadata);
//                    case Throwable t =>
//                        BrokerTopicStats.getBrokerTopicStats(topic).failedFetchRequestRate.mark();
//                        BrokerTopicStats.getBrokerAllTopicsStats.failedFetchRequestRate.mark();
//                        error("Error when processing fetch request for partition <%s,%d> offset %d from %s with correlation id %d. Possible cause: %s";
//                                .format(topic, partition, offset, if (isFetchFromFollower) "follower" else "consumer", fetchRequest.correlationId, t.getMessage))
//                        new PartitionDataAndOffset(new FetchResponsePartitionData(ErrorMapping.codeFor(t.getClass.asInstanceOf<Class<Throwable>>), -1L, MessageSet.Empty), LogOffsetMetadata.UnknownOffsetMetadata);
//                }
//                (TopicAndPartition(topic, partition), partitionDataAndOffsetInfo);
//            }
//        }
//
//        /**
//         * Read from a single topic/partition at the given offset upto maxSize bytes
//         */
//    private public void  readMessageSet(String topic,
//                               Integer partition,
//                               Long offset,
//                               Integer maxSize,
//                               Integer fromReplicaId): (FetchDataInfo, Long) = {
//        // check if the current broker is the leader for the partitions;
//        val localReplica = if(fromReplicaId == Request.DebuggingConsumerId)
//            getReplicaOrException(topic, partition);
//        else;
//            getLeaderReplicaIfLocal(topic, partition);
//        trace("Fetching log segment for topic, partition, offset, size = " + (topic, partition, offset, maxSize))
//        val maxOffsetOpt =
//        if (Request.isValidBrokerId(fromReplicaId))
//            None;
//        else;
//            Some(localReplica.highWatermark.messageOffset);
//        val fetchInfo = localReplica.log match {
//            case Some(log) =>
//                log.read(offset, maxSize, maxOffsetOpt);
//            case None =>
//                error(String.format("Leader for partition <%s,%d> does not have a local log",topic, partition))
//                FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MessageSet.Empty);
//        }
//        (fetchInfo, localReplica.highWatermark.messageOffset);
//    }
//
//    public void  maybeUpdateMetadataCache(UpdateMetadataRequest updateMetadataRequest, MetadataCache metadataCache) {
//        replicaStateChangeLock synchronized {
//            if(updateMetadataRequest.controllerEpoch < controllerEpoch) {
//                val stateControllerEpochErrorMessage = ("Broker %d received update metadata request with correlation id %d from an " +;
//                        "old controller %d with epoch %d. Latest known controller epoch is %d").format(localBrokerId,
//                        updateMetadataRequest.correlationId, updateMetadataRequest.controllerId, updateMetadataRequest.controllerEpoch,
//                        controllerEpoch);
//                stateChangeLogger.warn(stateControllerEpochErrorMessage);
//                throw new ControllerMovedException(stateControllerEpochErrorMessage);
//            } else {
//                metadataCache.updateCache(updateMetadataRequest, localBrokerId, stateChangeLogger);
//                controllerEpoch = updateMetadataRequest.controllerEpoch;
//            }
//        }
//    }
//
//    public void  becomeLeaderOrFollower(LeaderAndIsrRequest leaderAndISRRequest,
//                               OffsetManager offsetManager): (collection.Map[(String, Int), Short], Short) = {
//        leaderAndISRRequest.partitionStateInfos.foreach { case ((topic, partition), stateInfo) =>
//            stateChangeLogger.trace("Broker %d received LeaderAndIsr request %s correlation id %d from controller %d epoch %d for partition <%s,%d>";
//                    .format(localBrokerId, stateInfo, leaderAndISRRequest.correlationId,
//                            leaderAndISRRequest.controllerId, leaderAndISRRequest.controllerEpoch, topic, partition));
//        }
//        replicaStateChangeLock synchronized {
//            val responseMap = new collection.mutable.HashMap[(String, Int), Short];
//            if(leaderAndISRRequest.controllerEpoch < controllerEpoch) {
//                leaderAndISRRequest.partitionStateInfos.foreach { case ((topic, partition), stateInfo) =>
//                    stateChangeLogger.warn(("Broker %d ignoring LeaderAndIsr request from controller %d with correlation id %d since " +;
//                            "its controller epoch %d is old. Latest known controller epoch is %d").format(localBrokerId, leaderAndISRRequest.controllerId,
//                            leaderAndISRRequest.correlationId, leaderAndISRRequest.controllerEpoch, controllerEpoch));
//                }
//                (responseMap, ErrorMapping.StaleControllerEpochCode);
//            } else {
//                val controllerId = leaderAndISRRequest.controllerId;
//                val correlationId = leaderAndISRRequest.correlationId;
//                controllerEpoch = leaderAndISRRequest.controllerEpoch;
//
//                // First check partition's leader epoch;
//                val partitionState = new HashMap<Partition, PartitionStateInfo>();
//                leaderAndISRRequest.partitionStateInfos.foreach{ case ((topic, partitionId), partitionStateInfo) =>
//                    val partition = getOrCreatePartition(topic, partitionId);
//                    val partitionLeaderEpoch = partition.getLeaderEpoch();
//                    // If the leader epoch is valid record the epoch of the controller that made the leadership decision.;
//                    // This is useful while updating the isr to maintain the decision maker controller's epoch in the zookeeper path;
//                    if (partitionLeaderEpoch < partitionStateInfo.leaderIsrAndControllerEpoch.leaderAndIsr.leaderEpoch) {
//                        if(partitionStateInfo.allReplicas.contains(config.brokerId))
//                            partitionState.put(partition, partitionStateInfo);
//                        else {
//                            stateChangeLogger.warn(("Broker %d ignoring LeaderAndIsr request from controller %d with correlation id %d " +;
//                                    "epoch %d for partition <%s,%d> as itself is not in assigned replica list %s")
//                                    .format(localBrokerId, controllerId, correlationId, leaderAndISRRequest.controllerEpoch,
//                                            topic, partition.partitionId, partitionStateInfo.allReplicas.mkString(",")));
//                        }
//                    } else {
//                        // Otherwise record the error code in response;
//                        stateChangeLogger.warn(("Broker %d ignoring LeaderAndIsr request from controller %d with correlation id %d " +;
//                                "epoch %d for partition <%s,%d> since its associated leader epoch %d is old. Current leader epoch is %d")
//                                .format(localBrokerId, controllerId, correlationId, leaderAndISRRequest.controllerEpoch,
//                                        topic, partition.partitionId, partitionStateInfo.leaderIsrAndControllerEpoch.leaderAndIsr.leaderEpoch, partitionLeaderEpoch));
//                        responseMap.put((topic, partitionId), ErrorMapping.StaleLeaderEpochCode);
//                    }
//                }
//
//                val partitionsTobeLeader = partitionState;
//                        .filter{ case (partition, partitionStateInfo) => partitionStateInfo.leaderIsrAndControllerEpoch.leaderAndIsr.leader == config.brokerId}
//                val partitionsToBeFollower = (partitionState -- partitionsTobeLeader.keys);
//
//                if (!partitionsTobeLeader.isEmpty)
//                    makeLeaders(controllerId, controllerEpoch, partitionsTobeLeader, leaderAndISRRequest.correlationId, responseMap, offsetManager);
//                if (!partitionsToBeFollower.isEmpty)
//                    makeFollowers(controllerId, controllerEpoch, partitionsToBeFollower, leaderAndISRRequest.leaders, leaderAndISRRequest.correlationId, responseMap, offsetManager);
//
//                // we initialize highwatermark thread after the first leaderisrrequest. This ensures that all the partitions;
//                // have been completely populated before starting the checkpointing there by avoiding weird race conditions;
//                if (!hwThreadInitialized) {
//                    startHighWaterMarksCheckPointThread();
//                    hwThreadInitialized = true;
//                }
//                replicaFetcherManager.shutdownIdleFetcherThreads();
//                (responseMap, ErrorMapping.NoError);
//            }
//        }
//    }
//
//    /*
//     * Make the current broker to become leader for a given set of partitions by:
//     *
//     * 1. Stop fetchers for these partitions
//     * 2. Update the partition metadata in cache
//     * 3. Add these partitions to the leader partitions set
//     *
//     * If an unexpected error is thrown in this function, it will be propagated to KafkaApis where
//     * the error message will be set on each partition since we do not know which partition caused it
//     *  the TODO above may need to be fixed later
//     */
//    private public void  makeLeaders Integer controllerId, Integer epoch,
//                            Map partitionState<Partition, PartitionStateInfo>,
//                            Integer correlationId, mutable responseMap.Map[(String, Int), Short],
//    OffsetManager offsetManager) = {
//        partitionState.foreach(state =>
//                stateChangeLogger.trace(("Broker %d handling LeaderAndIsr request correlationId %d from controller %d epoch %d " +;
//                        "starting the become-leader transition for partition %s")
//                        .format(localBrokerId, correlationId, controllerId, epoch, TopicAndPartition(state._1.topic, state._1.partitionId))))
//
//        for (partition <- partitionState.keys)
//            responseMap.put((partition.topic, partition.partitionId), ErrorMapping.NoError);
//
//        try {
//            // First stop fetchers for all the partitions;
//            replicaFetcherManager.removeFetcherForPartitions(partitionState.keySet.map(new TopicAndPartition(_)));
//            partitionState.foreach { state =>
//                stateChangeLogger.trace(("Broker %d stopped fetchers as part of become-leader request from controller " +;
//                        "%d epoch %d with correlation id %d for partition %s")
//                        .format(localBrokerId, controllerId, epoch, correlationId, TopicAndPartition(state._1.topic, state._1.partitionId)))
//            }
//            // Update the partition information to be the leader;
//            partitionState.foreach{ case (partition, partitionStateInfo) =>
//                partition.makeLeader(controllerId, partitionStateInfo, correlationId, offsetManager)}
//
//        } catch {
//            case Throwable e =>
//                partitionState.foreach { state =>
//                val errorMsg = ("Error on broker %d while processing LeaderAndIsr request correlationId %d received from controller %d" +;
//                        " epoch %d for partition %s").format(localBrokerId, correlationId, controllerId, epoch,
//                        TopicAndPartition(state._1.topic, state._1.partitionId));
//                stateChangeLogger.error(errorMsg, e);
//            }
//            // Re-throw the exception for it to be caught in KafkaApis;
//            throw e;
//        }
//
//        partitionState.foreach { state =>
//            stateChangeLogger.trace(("Broker %d completed LeaderAndIsr request correlationId %d from controller %d epoch %d " +;
//                    "for the become-leader transition for partition %s")
//                    .format(localBrokerId, correlationId, controllerId, epoch, TopicAndPartition(state._1.topic, state._1.partitionId)))
//        }
//    }
//
//    /*
//     * Make the current broker to become follower for a given set of partitions by:
//     *
//     * 1. Remove these partitions from the leader partitions set.
//     * 2. Mark the replicas as followers so that no more data can be added from the producer clients.
//     * 3. Stop fetchers for these partitions so that no more data can be added by the replica fetcher threads.
//     * 4. Truncate the log and checkpoint offsets for these partitions.
//     * 5. If the broker is not shutting down, add the fetcher to the new leaders.
//     *
//     * The ordering of doing these steps make sure that the replicas in transition will not
//     * take any more messages before checkpointing offsets so that all messages before the checkpoint
//     * are guaranteed to be flushed to disks
//     *
//     * If an unexpected error is thrown in this function, it will be propagated to KafkaApis where
//     * the error message will be set on each partition since we do not know which partition caused it
//     */
//    private public void  makeFollowers Integer controllerId, Integer epoch, Map partitionState<Partition, PartitionStateInfo>,
//                              Set leaders<Broker>, Integer correlationId, mutable responseMap.Map[(String, Int), Short],
//    OffsetManager offsetManager) {
//        partitionState.foreach { state =>
//            stateChangeLogger.trace(("Broker %d handling LeaderAndIsr request correlationId %d from controller %d epoch %d " +;
//                    "starting the become-follower transition for partition %s")
//                    .format(localBrokerId, correlationId, controllerId, epoch, TopicAndPartition(state._1.topic, state._1.partitionId)))
//        }
//
//        for (partition <- partitionState.keys)
//            responseMap.put((partition.topic, partition.partitionId), ErrorMapping.NoError);
//
//        try {
//
//            var Set partitionsToMakeFollower<Partition> = Set();
//
//            // Delete TODO leaders from LeaderAndIsrRequest in 0.8.1;
//            partitionState.foreach{ case (partition, partitionStateInfo) =>
//                val leaderIsrAndControllerEpoch = partitionStateInfo.leaderIsrAndControllerEpoch;
//                val newLeaderBrokerId = leaderIsrAndControllerEpoch.leaderAndIsr.leader;
//                leaders.find(_.id == newLeaderBrokerId) match {
//                // Only change partition state when the leader is available;
//                case Some(leaderBroker) =>
//                    if (partition.makeFollower(controllerId, partitionStateInfo, correlationId, offsetManager))
//                        partitionsToMakeFollower += partition;
//                    else;
//                        stateChangeLogger.info(("Broker %d skipped the become-follower state change after marking its partition as follower with correlation id %d from " +;
//                                "controller %d epoch %d for partition <%s,%d> since the new leader %d is the same as the old leader")
//                                .format(localBrokerId, correlationId, controllerId, leaderIsrAndControllerEpoch.controllerEpoch,
//                                        partition.topic, partition.partitionId, newLeaderBrokerId));
//                case None =>
//                    // The leader broker should always be present in the leaderAndIsrRequest.;
//                    // If not, we should record the error message and abort the transition process for this partition;
//                    stateChangeLogger.error(("Broker %d received LeaderAndIsrRequest with correlation id %d from controller" +;
//                            " %d epoch %d for partition <%s,%d> but cannot become follower since the new leader %d is unavailable.")
//                            .format(localBrokerId, correlationId, controllerId, leaderIsrAndControllerEpoch.controllerEpoch,
//                                    partition.topic, partition.partitionId, newLeaderBrokerId));
//                    // Create the local replica even if the leader is unavailable. This is required to ensure that we include;
//                    // the partition's high watermark in the checkpoint file (see KAFKA-1647);
//                    partition.getOrCreateReplica();
//            }
//            }
//
//            replicaFetcherManager.removeFetcherForPartitions(partitionsToMakeFollower.map(new TopicAndPartition(_)));
//            partitionsToMakeFollower.foreach { partition =>
//                stateChangeLogger.trace(("Broker %d stopped fetchers as part of become-follower request from controller " +;
//                        "%d epoch %d with correlation id %d for partition %s")
//                        .format(localBrokerId, controllerId, epoch, correlationId, TopicAndPartition(partition.topic, partition.partitionId)))
//            }
//
//            logManager.truncateTo(partitionsToMakeFollower.map(partition => (new TopicAndPartition(partition), partition.getOrCreateReplica().highWatermark.messageOffset)).toMap);
//
//            partitionsToMakeFollower.foreach { partition =>
//                stateChangeLogger.trace(("Broker %d truncated logs and checkpointed recovery boundaries for partition <%s,%d> as part of " +;
//                        "become-follower request with correlation id %d from controller %d epoch %d").format(localBrokerId,
//                        partition.topic, partition.partitionId, correlationId, controllerId, epoch));
//            }
//
//            if (isShuttingDown.get()) {
//                partitionsToMakeFollower.foreach { partition =>
//                    stateChangeLogger.trace(("Broker %d skipped the adding-fetcher step of the become-follower state change with correlation id %d from " +;
//                            "controller %d epoch %d for partition <%s,%d> since it is shutting down").format(localBrokerId, correlationId,
//                            controllerId, epoch, partition.topic, partition.partitionId));
//                }
//            }
//            else {
//                // we do not need to check if the leader exists again since this has been done at the beginning of this process;
//                val partitionsToMakeFollowerWithLeaderAndOffset = partitionsToMakeFollower.map(partition =>
//                        new TopicAndPartition(partition) -> BrokerAndInitialOffset(
//                        leaders.find(_.id == partition.leaderReplicaIdOpt.get).get,
//                        partition.getReplica().get.logEndOffset.messageOffset)).toMap;
//                replicaFetcherManager.addFetcherForPartitions(partitionsToMakeFollowerWithLeaderAndOffset);
//
//                partitionsToMakeFollower.foreach { partition =>
//                    stateChangeLogger.trace(("Broker %d started fetcher to new leader as part of become-follower request from controller " +;
//                            "%d epoch %d with correlation id %d for partition <%s,%d>")
//                            .format(localBrokerId, controllerId, epoch, correlationId, partition.topic, partition.partitionId))
//                }
//            }
//        } catch {
//            case Throwable e =>
//                val errorMsg = ("Error on broker %d while processing LeaderAndIsr request with correlationId %d received from controller %d " +;
//                        "epoch %d").format(localBrokerId, correlationId, controllerId, epoch)
//                stateChangeLogger.error(errorMsg, e);
//                // Re-throw the exception for it to be caught in KafkaApis;
//                throw e;
//        }
//
//        partitionState.foreach { state =>
//            stateChangeLogger.trace(("Broker %d completed LeaderAndIsr request correlationId %d from controller %d epoch %d " +;
//                    "for the become-follower transition for partition %s")
//                    .format(localBrokerId, correlationId, controllerId, epoch, TopicAndPartition(state._1.topic, state._1.partitionId)))
//        }
//    }
//
//    private public Unit  void  maybeShrinkIsr() {
//        trace("Evaluating ISR list of partitions to see which replicas can be removed from the ISR");
//        allPartitions.values.foreach(partition => partition.maybeShrinkIsr(config.replicaLagTimeMaxMs, config.replicaLagMaxMessages))
//    }
//
//    public void  updateReplicaLEOAndPartitionHW(String topic, Integer partitionId, Integer replicaId, LogOffsetMetadata offset) = {
//        getPartition(topic, partitionId) match {
//            case Some(partition) =>
//                partition.getReplica(replicaId) match {
//                case Some(replica) =>
//                    replica.logEndOffset = offset;
//                    // check if we need to update HW and expand Isr;
//                    partition.updateLeaderHWAndMaybeExpandIsr(replicaId);
//                    debug(String.format("Recorded follower %d position %d for partition <%s,%d>.",replicaId, offset.messageOffset, topic, partitionId))
//                case None =>
//                    throw new NotAssignedReplicaException(("Leader %d failed to record follower %d's position %d since the replica" +;
//                            " is not recognized to be one of the assigned replicas %s for partition <%s,%d>").format(localBrokerId, replicaId,
//                            offset.messageOffset, partition.assignedReplicas().map(_.brokerId).mkString(","), topic, partitionId));
//
//            }
//            case None =>
//                warn(String.format("While recording the follower position, the partition <%s,%d> hasn't been created, skip updating leader HW",topic, partitionId))
//        }
//    }
//
//    private public void  getLeaderPartitions() : List<Partition> = {
//        allPartitions.values.filter(_.leaderReplicaIfLocal().isDefined).toList;
//    }
//    /**
//     * Flushes the highwatermark value for all partitions to the highwatermark file
//     */
//    public void  checkpointHighWatermarks() {
//        val replicas = allPartitions.values.map(_.getReplica(config.brokerId)).collect{case Some(replica) => replica}
//        val replicasByDir = replicas.filter(_.log.isDefined).groupBy(_.log.get.dir.getParentFile.getAbsolutePath);
//        for((dir, reps) <- replicasByDir) {
//            val hwms = reps.map(r => (new TopicAndPartition(r) -> r.highWatermark.messageOffset)).toMap;
//            try {
//                highWatermarkCheckpoints(dir).write(hwms);
//            } catch {
//                case IOException e =>
//                    fatal("Error writing to highwatermark file: ", e);
//                    Runtime.getRuntime().halt(1);
//            }
//        }
//    }
//
//    public void  shutdown() {
//        info("Shut down");
//        replicaFetcherManager.shutdown();
//        checkpointHighWatermarks();
//        info("Shut down completely");
//    }
//}
}
