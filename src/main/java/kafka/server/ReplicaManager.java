package kafka.server;

import com.google.common.collect.Maps;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import kafka.api.FetchRequest;
import kafka.api.FetchResponsePartitionData;
import kafka.api.Request;
import kafka.api.StopReplicaRequest;
import kafka.cluster.Partition;
import kafka.cluster.Replica;
import kafka.common.ErrorMapping;
import kafka.common.ReplicaNotAvailableException;
import kafka.func.Tuple;
import kafka.log.LogManager;
import kafka.log.OffsetCheckpoint;
import kafka.log.TopicAndPartition;
import kafka.message.ByteBufferMessageSet;
import kafka.message.MessageSet;
import kafka.metrics.KafkaMetricsGroup;
import kafka.utils.Pool;
import kafka.utils.Scheduler;
import kafka.utils.Time;
import kafka.utils.Utils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.common.errors.NotLeaderForPartitionException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Created by zhoulf on 2017/4/1.
 */


public class ReplicaManager extends KafkaMetricsGroup {
    public static final String HighWatermarkFilename = "replication-offset-checkpoint";

    public KafkaConfig config;
    public Time time;
    public ZkClient zkClient;
    public  Scheduler scheduler;
    public LogManager logManager;
    public  AtomicBoolean isShuttingDown;

    public ReplicaManager(KafkaConfig config, Time time, ZkClient zkClient, Scheduler scheduler, LogManager logManager, AtomicBoolean isShuttingDown) {
        this.config = config;
        this.time = time;
        this.zkClient = zkClient;
        this.scheduler = scheduler;
        this.logManager = logManager;
        this.isShuttingDown = isShuttingDown;
    }

    /* epoch of the controller that last changed the leader */
    public volatile Integer controllerEpoch = KafkaController.InitialControllerEpoch - 1;
    private Integer localBrokerId = config.brokerId;
    private Pool<Tuple<String, Integer>, Partition> allPartitions = new Pool<>();
    private Object replicaStateChangeLock = new Object();
    //        public ReplicaFetcherManager replicaFetcherManager = new ReplicaFetcherManager(config, this);
    private AtomicBoolean highWatermarkCheckPointThreadStarted = new AtomicBoolean(false);
    Map<File, OffsetCheckpoint> highWatermarkCheckpoints = null;

    public void init() {
        highWatermarkCheckpoints = Utils.toMap(config.logDirs.stream().map(dir -> {
            try {
                return Tuple.of(new File(dir).getAbsolutePath(), new OffsetCheckpoint(new File(dir, ReplicaManager.HighWatermarkFilename)));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).collect(Collectors.toList()));
        this.logIdent = "[Replica Manager on Broker " + localBrokerId + "]: ";

        newGauge("LeaderCount", new Gauge<Integer>() {
                    public  Integer value() {
                        return getLeaderPartitions().size;
                    }
                });
        newGauge("PartitionCount", new Gauge<Integer>() {
            public Integer value(){return allPartitions.getSize();}
        });
        newGauge("UnderReplicatedPartitions", new Gauge<Integer>() {
            public Integer value(){ return  underReplicatedPartitionCount();}
        });
    }

    private boolean hwThreadInitialized = false;

    StateChangeLogger stateChangeLogger = KafkaController.stateChangeLogger;
//
//         ProducerRequestPurgatory producerRequestPurgatory = null;
//         FetchRequestPurgatory fetchRequestPurgatory = null;
//
//
        Meter isrExpandRate = newMeter("IsrExpandsPerSec", "expands", TimeUnit.SECONDS);
        Meter isrShrinkRate = newMeter("IsrShrinksPerSec",  "shrinks", TimeUnit.SECONDS);
//
        public Integer   underReplicatedPartitionCount() {
                return getLeaderPartitions().count(_.isUnderReplicated);
        }

    public void startHighWaterMarksCheckPointThread() {
        if (highWatermarkCheckPointThreadStarted.compareAndSet(false, true))
            scheduler.schedule("highwatermark-checkpoint", checkpointHighWatermarks, period = config.replicaHighWatermarkCheckpointIntervalMs, unit = TimeUnit.MILLISECONDS);
    }

        /**
         * Initialize the replica manager with the request purgatory
         *
         * will TODO be removed in 0.9 where we refactor server structure
         */
//
        public void  initWithRequestPurgatory(ProducerRequestPurgatory producerRequestPurgatory, FetchRequestPurgatory fetchRequestPurgatory) {
            this.producerRequestPurgatory = producerRequestPurgatory;
            this.fetchRequestPurgatory = fetchRequestPurgatory;
        }

        /**
         * Unblock some delayed produce requests with the request key
         */
        public void  unblockDelayedProduceRequests(TopicAndPartition key) {
            val satisfied = producerRequestPurgatory.update(key);
            debug(String.format("Request key %s unblocked %d producer requests.",key, satisfied.size));

            // send any newly unblocked responses;
            satisfied.foreach(producerRequestPurgatory.respond(_));
        }

        /**
         * Unblock some delayed fetch requests with the request key
         */
        public void  unblockDelayedFetchRequests(TopicAndPartition key) {
            val satisfied = fetchRequestPurgatory.update(key);
            debug(String.format("Request key %s unblocked %d fetch requests.",key, satisfied.size))

            // send any newly unblocked responses;
            satisfied.foreach(fetchRequestPurgatory.respond(_))
        }

        public void  startup() {
            // start ISR expiration thread;
            scheduler.schedule("isr-expiration", maybeShrinkIsr, period = config.replicaLagTimeMaxMs, unit = TimeUnit.MILLISECONDS);
        }

    public Short stopReplica(String topic, Integer partitionId, Boolean deletePartition) {
        stateChangeLogger.trace(String.format("Broker %d handling stop replica (delete=%s) for partition <%s,%d>", localBrokerId,
                deletePartition.toString(), topic, partitionId));
        Short errorCode = ErrorMapping.NoError;
        Optional<Partition> optional = getPartition(topic, partitionId);
        if (optional.isPresent()) {
            if (deletePartition) {
                Partition removedPartition = allPartitions.remove(Tuple.of(topic, partitionId));
                if (removedPartition != null)
                    removedPartition.delete(); // this will delete the local log;
            }
        } else {
            // Delete log and corresponding folders in case replica manager doesn't hold them anymore.;
            // This could happen when topic is being deleted while broker is down and recovers.;
            if (deletePartition) {
                TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partitionId);
                if (logManager.getLog(topicAndPartition).isPresent()) {
                    logManager.deleteLog(topicAndPartition);
                }
            }
        }
        stateChangeLogger.trace(String.format("Broker %d ignoring stop replica (delete=%s) for partition <%s,%d> as replica doesn't exist on broker", localBrokerId, deletePartition, topic, partitionId));
        stateChangeLogger.trace(String.format("Broker %d finished handling stop replica (delete=%s) for partition <%s,%d>", localBrokerId, deletePartition, topic, partitionId));
        return errorCode;
    }

            public Tuple<Map<TopicAndPartition, Short>, Short> stopReplicas(StopReplicaRequest stopReplicaRequest){
             synchronized(replicaStateChangeLock) {
                 Map<TopicAndPartition, Short> responseMap = Maps.newHashMap();
                if(stopReplicaRequest.controllerEpoch < controllerEpoch) {
                    stateChangeLogger.warn(String.format("Broker %d received stop replica request from an old controller epoch %d."+
                            localBrokerId, stopReplicaRequest.controllerEpoch) +
                            " Latest known controller epoch is %d " + controllerEpoch);
                    return Tuple.of(responseMap, ErrorMapping.StaleControllerEpochCode);
                } else {
                    controllerEpoch = stopReplicaRequest.controllerEpoch;
                    // First stop fetchers for all partitions, then stop the corresponding replicas;
                    replicaFetcherManager.removeFetcherForPartitions(stopReplicaRequest.partitions.map(r => TopicAndPartition(r.topic, r.partition)));
                    for(TopicAndPartition topicAndPartition : stopReplicaRequest.partitions){
                        short errorCode = stopReplica(topicAndPartition.topic, topicAndPartition.partition, stopReplicaRequest.deletePartitions);
                        responseMap.put(topicAndPartition, errorCode);
                    }
                    return Tuple.of(responseMap, ErrorMapping.NoError);
                }
            }
        }

        public Partition  getOrCreatePartition(String topic, Integer partitionId) {
                Partition partition = allPartitions.get(Tuple.of(topic, partitionId));
        if (partition == null) {
            allPartitions.putIfNotExists(Tuple.of(topic, partitionId), new Partition(topic, partitionId, time, this));
            partition = allPartitions.get(Tuple.of(topic, partitionId));
        }
        return partition;
  }

    public Optional<Partition> getPartition(String topic, Integer partitionId) {
        Partition partition = allPartitions.get(Tuple.of(topic, partitionId));
        if (partition == null)
            return Optional.empty();
        else
            return Optional.of(partition);
    }

        public Replica    getReplicaOrException(String topic, Integer partition) {
                Optional<Replica> replicaOpt = getReplica(topic, partition,null);
        if(replicaOpt.isPresent())
            return replicaOpt.get();
        else;
            throw new ReplicaNotAvailableException(String.format("Replica %d is not available for partition <%s,%d>",config.brokerId, topic, partition))
  }

        public Replica getLeaderReplicaIfLocal(String topic, Integer partitionId)  {
                Optional<Partition> partitionOpt = getPartition(topic, partitionId);
                if(partitionOpt.isPresent()){
                    Partition partition = partitionOpt.get();
                    Optional<Replica> leaderReplicaIfLocalOpt = partition.leaderReplicaIfLocal();
                    if (leaderReplicaIfLocalOpt.isPresent()){
                        return leaderReplicaIfLocalOpt.get();
                    }else{
                        throw new NotLeaderForPartitionException(String.format("Leader not local for partition <%s,%d> on broker %d".format(topic, partitionId, config.brokerId));
                    }
                }
             throw new UnknownTopicOrPartitionException(String.format("Partition <%s,%d> doesn't exist on %d",topic, partitionId, config.brokerId))
          }

        public Optional<Replica>  getReplica(String topic, Integer partitionId, Integer replicaId ){
            if(replicaId==null){
                replicaId = config.brokerId;
            }
            Optional<Partition> partitionOpt = getPartition(topic, partitionId);
            if(partitionOpt.isPresent()){
               return partitionOpt.get().getReplica(replicaId);
            }
            return Optional.empty();
    }
  }
class PartitionDataAndOffset {
    (: FetchResponsePartitionData data;
    LogOffsetMetadata offset;
}
            /**
             * Read from all the offset details given and return a map of
             * (topic, partition) -> PartitionData
             */
            public Tuple< TopicAndPartition, PartitionDataAndOffset> readMessageSets(FetchRequest fetchRequest) {
            boolean isFetchFromFollower = fetchRequest.isFromFollower();
           return Utils.map2(fetchRequest.requestInfo, kv-> {
                String topic = kv.getKey().topic;
                Integer partition = kv.getKey().partition;
                Long offset = kv.getValue().offset;
                Integer fetchSize = kv.getValue().fetchSize;
                PartitionDataAndOffset partitionDataAndOffsetInfo;
                try {
                    val(fetchInfo, highWatermark) = readMessageSet(topic, partition, offset, fetchSize, fetchRequest.replicaId);
                    BrokerTopicStats.getBrokerTopicStats(topic).bytesOutRate.mark(fetchInfo.messageSet.sizeInBytes);
                    BrokerTopicStats.getBrokerAllTopicsStats.bytesOutRate.mark(fetchInfo.messageSet.sizeInBytes);
                    if (isFetchFromFollower) {
                        debug(String.format("Partition <%s,%d> received fetch request from follower %d",topic, partition, fetchRequest.replicaId));
                    }
                    partitionDataAndOffsetInfo= new PartitionDataAndOffset(new FetchResponsePartitionData(ErrorMapping.NoError, highWatermark, fetchInfo.messageSet), fetchInfo.fetchOffset);
                } catch (UnknownTopicOrPartitionException utpe) {
                    // Failed NOTE fetch requests is not incremented for UnknownTopicOrPartitionException and NotLeaderForPartitionException;
                    // since failed fetch requests metric is supposed to indicate failure of a broker in handling a fetch request;
                    // for a partition it is the leader for;
                    warn(String.format("Fetch request with correlation id %d from client %s on partition <%s,%d> failed due to %s",
                            fetchRequest.correlationId, fetchRequest.clientId, topic, partition, utpe.getMessage));
                    partitionDataAndOffsetInfo = new PartitionDataAndOffset(new FetchResponsePartitionData(ErrorMapping.codeFor(utpe.getClass.asInstanceOf < Class < Throwable >>), -1L, MessageSet.Empty), LogOffsetMetadata.UnknownOffsetMetadata);
                } catch (NotLeaderForPartitionException nle){
                        warn(String.format("Fetch request with correlation id %d from client %s on partition <%s,%d> failed due to %s",
                                fetchRequest.correlationId, fetchRequest.clientId, topic, partition, nle.getMessage));
                    partitionDataAndOffsetInfo =   new PartitionDataAndOffset(new FetchResponsePartitionData(ErrorMapping.codeFor(nle.getClass.asInstanceOf < Class < Throwable >>), -1L, MessageSet.Empty), LogOffsetMetadata.UnknownOffsetMetadata);
                }  catch (Throwable t){
                        BrokerTopicStats.getBrokerTopicStats(topic).failedFetchRequestRate.mark();
                        BrokerTopicStats.getBrokerAllTopicsStats().failedFetchRequestRate.mark();
                        error(String.format("Error when processing fetch request for partition <%s,%d> offset %d from %s with correlation id %d. Possible cause: %s"+
                                topic, partition, offset,isFetchFromFollower? "follower": "consumer", fetchRequest.correlationId, t.getMessage()));
                    partitionDataAndOffsetInfo =   new PartitionDataAndOffset(new FetchResponsePartitionData(ErrorMapping.codeFor(t.getClass.asInstanceOf < Class < Throwable >>), -1L, MessageSet.Empty), LogOffsetMetadata.UnknownOffsetMetadata);
                }
                return Tuple.of(new TopicAndPartition(topic, partition), partitionDataAndOffsetInfo);
            });
        }
//
        /**
         * Read from a single topic/partition at the given offset upto maxSize bytes
         */
    private  Tuple<FetchDataInfo, Long>  readMessageSet(String topic,
                               Integer partition,
                               Long offset,
                               Integer maxSize,
                               Integer fromReplicaId){
        // check if the current broker is the leader for the partitions;
        Replica localReplica;
        if(fromReplicaId == Request.DebuggingConsumerId)
            localReplica=getReplicaOrException(topic, partition);
        else
            localReplica=getLeaderReplicaIfLocal(topic, partition);
        trace("Fetching log segment for topic, partition, offset, size = " + (topic, partition, offset, maxSize))
        Optional<Long> maxOffsetOpt;
        if (Request.isValidBrokerId(fromReplicaId))
            maxOffsetOpt=Optional.empty();
        else
            maxOffsetOpt=Optional.of(localReplica.highWatermark.messageOffset);
        FetchDataInfo fetchInfo;
        if( localReplica.log.isPresent()) {
            fetchInfo=localReplica.log.get().read(offset, maxSize, maxOffsetOpt);
        }else{
            error(String.format("Leader for partition <%s,%d> does not have a local log",topic, partition));
            fetchInfo=new FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MessageSet.Empty);
        }
        return Tuple.of(fetchInfo, localReplica.highWatermark.messageOffset);
    }
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
    private List<Partition> getLeaderPartitions() {
        return Utils.filter(allPartitions.values(),p->p.leaderReplicaIfLocal().isPresent());
    }
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
    public void  shutdown() {
        info("Shut down");
        replicaFetcherManager.shutdown();
        checkpointHighWatermarks();
        info("Shut down completely");
    }
//}
}
