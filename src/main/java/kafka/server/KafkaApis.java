package kafka.server;/**
 * Created by zhoulf on 2017/5/11.
 */

import kafka.api.OffsetCommitRequest;
import kafka.api.ProducerRequest;
import kafka.api.RequestKeys;
import kafka.common.ErrorMapping;
import kafka.common.OffsetAndMetadata;
import kafka.log.TopicAndPartition;
import kafka.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageSet;
import kafka.network.RequestChannel;
import kafka.utils.Logging;
import kafka.utils.SystemTime;
import kafka.utils.Time;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.consumer.OffsetMetadata;
import org.apache.kafka.common.errors.InvalidTopicException;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * Logic to handle the various Kafka requests
 */
public class KafkaApis extends Logging {


    RequestChannel requestChannel;
    ReplicaManager replicaManager;
    OffsetManager offsetManager;
    ZkClient zkClient;
    Integer brokerId;
    KafkaConfig config;
    KafkaController controller;
//
//    public KafkaApis(RequestChannel requestChannel, ReplicaManager replicaManager, OffsetManager offsetManager, ZkClient zkClient, Integer brokerId, KafkaConfig config, KafkaController controller) {
//        this.requestChannel = requestChannel;
//        this.replicaManager = replicaManager;
//        this.offsetManager = offsetManager;
//        this.zkClient = zkClient;
//        this.brokerId = brokerId;
//        this.config = config;
//        this.controller = controller;
//        //        val producerRequestPurgatory = new ProducerRequestPurgatory(replicaManager, offsetManager, requestChannel);
////        val fetchRequestPurgatory = new FetchRequestPurgatory(replicaManager, requestChannel);
////        // the TODO following line will be removed in 0.9;
////        replicaManager.initWithRequestPurgatory(producerRequestPurgatory, fetchRequestPurgatory);
////        var metadataCache = new MetadataCache;
//        loggerName(String.format("<KafkaApi-%d> ",brokerId));
//    }

    /**
     * Top-level method that handles all requests and multiplexes to the right api
     */
    public void handle(RequestChannel.Request request) {
//            try{
//                trace("Handling request: " + request.requestObj + " from client: " + request.remoteAddress);
//                request.requestId match {
//                    case RequestKeys.ProduceKey => handleProducerOrOffsetCommitRequest(request);
//                    case RequestKeys.FetchKey => handleFetchRequest(request);
//                    case RequestKeys.OffsetsKey => handleOffsetRequest(request);
//                    case RequestKeys.MetadataKey => handleTopicMetadataRequest(request);
//                    case RequestKeys.LeaderAndIsrKey => handleLeaderAndIsrRequest(request);
//                    case RequestKeys.StopReplicaKey => handleStopReplicaRequest(request);
//                    case RequestKeys.UpdateMetadataKey => handleUpdateMetadataRequest(request);
//                    case RequestKeys.ControlledShutdownKey => handleControlledShutdownRequest(request);
//                    case RequestKeys.OffsetCommitKey => handleOffsetCommitRequest(request);
//                    case RequestKeys.OffsetFetchKey => handleOffsetFetchRequest(request);
//                    case RequestKeys.ConsumerMetadataKey => handleConsumerMetadataRequest(request);
//                    case requestId => throw new KafkaException("Unknown api code " + requestId);
//                }
//            } catch {
//                case Throwable e =>
//                    request.requestObj.handleError(e, requestChannel, request);
//                    error(String.format("error when handling request %s",request.requestObj), e)
//            } finally;
//            request.apiLocalCompleteTimeMs = SystemTime.milliseconds;
    }

    //
//       public void handleOffsetCommitRequest(RequestChannel request.Request) {
//            val offsetCommitRequest = request.requestObj.asInstanceOf<OffsetCommitRequest>
//            if (offsetCommitRequest.versionId == 0) {
//                // version 0 stores the offsets in ZK;
//                val responseInfo = offsetCommitRequest.requestInfo.map{
//                    case (topicAndPartition, metaAndError) => {
//                        val topicDirs = new ZKGroupTopicDirs(offsetCommitRequest.groupId, topicAndPartition.topic);
//                        try {
//                            ensureTopicExists(topicAndPartition.topic);
//                            if(metaAndError.metadata != null && metaAndError.metadata.length > config.offsetMetadataMaxSize) {
//                                (topicAndPartition, ErrorMapping.OffsetMetadataTooLargeCode);
//                            } else {
//                                ZkUtils.updatePersistentPath(zkClient, topicDirs.consumerOffsetDir + "/" +
//                                        topicAndPartition.partition, metaAndError.offset.toString);
//                                (topicAndPartition, ErrorMapping.NoError);
//                            }
//                        } catch {
//                            case Throwable e => (topicAndPartition, ErrorMapping.codeFor(e.getClass.asInstanceOf<Class<Throwable>>));
//                        }
//                    }
//                }
//                val response = new OffsetCommitResponse(responseInfo, offsetCommitRequest.correlationId);
//                requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)));
//            } else {
//                // version 1 and above store the offsets in a special Kafka topic;
//                handleProducerOrOffsetCommitRequest(request);
//            }
//        }
//
//    private void ensureTopicExists(String topic) = {
//        if (metadataCache.getTopicMetadata(Set(topic)).size <= 0)
//            throw new UnknownTopicOrPartitionException("Topic " + topic + " either doesn't exist or is in the process of being deleted");
//    }
//
//   public void handleLeaderAndIsrRequest(RequestChannel request.Request) {
//        // ensureTopicExists is only for client facing requests;
//        // We can't have the ensureTopicExists check here since the controller sends it as an advisory to all brokers so they;
//        // stop serving data to clients for the topic being deleted;
//        val leaderAndIsrRequest = request.requestObj.asInstanceOf<LeaderAndIsrRequest>
//        try {
//            val (response, error) = replicaManager.becomeLeaderOrFollower(leaderAndIsrRequest, offsetManager);
//            val leaderAndIsrResponse = new LeaderAndIsrResponse(leaderAndIsrRequest.correlationId, response, error);
//            requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend(leaderAndIsrResponse)));
//        } catch {
//            case KafkaStorageException e =>
//                fatal("Disk error during leadership change.", e);
//                Runtime.getRuntime.halt(1);
//        }
//    }
//
//   public void handleStopReplicaRequest(RequestChannel request.Request) {
//        // ensureTopicExists is only for client facing requests;
//        // We can't have the ensureTopicExists check here since the controller sends it as an advisory to all brokers so they;
//        // stop serving data to clients for the topic being deleted;
//        val stopReplicaRequest = request.requestObj.asInstanceOf<StopReplicaRequest>
//        val (response, error) = replicaManager.stopReplicas(stopReplicaRequest);
//        val stopReplicaResponse = new StopReplicaResponse(stopReplicaRequest.correlationId, response.toMap, error);
//        requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend(stopReplicaResponse)));
//        replicaManager.replicaFetcherManager.shutdownIdleFetcherThreads();
//    }
//
//   public void handleUpdateMetadataRequest(RequestChannel request.Request) {
//        val updateMetadataRequest = request.requestObj.asInstanceOf<UpdateMetadataRequest>
//        replicaManager.maybeUpdateMetadataCache(updateMetadataRequest, metadataCache);
//
//        val updateMetadataResponse = new UpdateMetadataResponse(updateMetadataRequest.correlationId);
//        requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend(updateMetadataResponse)));
//    }
//
//   public void handleControlledShutdownRequest(RequestChannel request.Request) {
//        // ensureTopicExists is only for client facing requests;
//        // We can't have the ensureTopicExists check here since the controller sends it as an advisory to all brokers so they;
//        // stop serving data to clients for the topic being deleted;
//        val controlledShutdownRequest = request.requestObj.asInstanceOf<ControlledShutdownRequest>
//        val partitionsRemaining = controller.shutdownBroker(controlledShutdownRequest.brokerId);
//        val controlledShutdownResponse = new ControlledShutdownResponse(controlledShutdownRequest.correlationId,
//                ErrorMapping.NoError, partitionsRemaining);
//        requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend(controlledShutdownResponse)));
//    }
//
    private ProducerRequest producerRequestFromOffsetCommit(OffsetCommitRequest offsetCommitRequest) {
        List<Message> msgs = offsetCommitRequest.filterLargeMetadata(config.offsetMetadataMaxSize).map(entry -> {
            TopicAndPartition topicAndPartition = entry.getKey();
            OffsetAndMetadata offset = entry.getValue();
            new Message(OffsetManager.offsetCommitValue(offset),
                    OffsetManager.offsetCommitKey(offsetCommitRequest.groupId, topicAndPartition.topic, topicAndPartition.partition));
        }).collect(Collectors.toList());

        val producerData = mutable.Map(
                TopicAndPartition(OffsetManager.OffsetsTopicName, offsetManager.partitionFor(offsetCommitRequest.groupId)) ->
        new ByteBufferMessageSet(config.offsetsTopicCompressionCodec, _ msgs *);
    );
//
//        val request = ProducerRequest(
//                correlationId = offsetCommitRequest.correlationId,
//                clientId = offsetCommitRequest.clientId,
//                requiredAcks = config.offsetCommitRequiredAcks,
//                ackTimeoutMs = config.offsetCommitTimeoutMs,
//                data = producerData);
//        trace(String.format("Created producer request %s for offset commit request %s.",request, offsetCommitRequest))
//        request;
//    }
//
        /**
         * Handle a produce request or offset commit request (which is really a specialized producer request)
         */

    public void handleProducerOrOffsetCommitRequest(RequestChannel.Request request) {
        ProducerRequest produceRequest;
        Optional<OffsetCommitRequest> offsetCommitRequestOpt;
        if (request.requestId == RequestKeys.OffsetCommitKey) {
            OffsetCommitRequest offsetCommitRequest = (OffsetCommitRequest) request.requestObj;
            OffsetCommitRequest.changeInvalidTimeToCurrentTime(offsetCommitRequest);
            produceRequest = producerRequestFromOffsetCommit(offsetCommitRequest);
            offsetCommitRequestOpt = Optional.of(offsetCommitRequest);
        } else {
            produceRequest = (ProducerRequest) request.requestObj;
            offsetCommitRequestOpt = Optional.empty();
        }

        if (produceRequest.requiredAcks > 1 || produceRequest.requiredAcks < -1) {
            warn(("Client %s from %s sent a produce request with request.required.acks of %d, which is now deprecated and will " +
                    "be removed in next release. Valid values are -1, 0 or 1. Please consult Kafka documentation for supported " +
                    "and recommended configuration.").format(produceRequest.clientId, request.remoteAddress, produceRequest.requiredAcks))
        }

        Long sTime = Time.get().milliseconds();
        Iterable<ProduceResult> localProduceResults = appendToLocalLog(produceRequest, offsetCommitRequestOpt.isPresent());
        debug(String.format("Produce to local log in %d ms", Time.get().milliseconds() - sTime));

        val firstErrorCode = localProduceResults.find(_.errorCode != ErrorMapping.NoError).map(_.errorCode).getOrElse(ErrorMapping.NoError);

        val numPartitionsInError = localProduceResults.count(_.error.isDefined);
        if (produceRequest.requiredAcks == 0) {
            // no operation needed if producer request.required.acks = 0; however, if there is any exception in handling the request, since;
            // no response is expected by the producer the handler will send a close connection response to the socket server;
            // to close the socket so that the producer client will know that some exception has happened and will refresh its metadata;
            if (numPartitionsInError != 0) {
                info(("Send the close connection response due to error handling produce request " +
                        "<clientId = %s, correlationId = %s, topicAndPartition = %s> with Ack=0");
                        .
                format(produceRequest.clientId, produceRequest.correlationId, produceRequest.topicPartitionMessageSizeMap.keySet.mkString(",")))
                requestChannel.closeConnection(request.processor, request);
            } else {

                if (firstErrorCode == ErrorMapping.NoError)
                    offsetCommitRequestOpt.foreach(ocr = > offsetManager.putOffsets(ocr.groupId, ocr.requestInfo))

                if (offsetCommitRequestOpt.isDefined) {
                    val response = offsetCommitRequestOpt.get.responseFor(firstErrorCode, config.offsetMetadataMaxSize);
                    requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)));
                } else ;
                requestChannel.noOperation(request.processor, request);
            }
        } else if ( produceRequest.requiredAcks == 1 ||;
        produceRequest.numPartitions <= 0 ||;
        numPartitionsInError == produceRequest.numPartitions){

            if (firstErrorCode == ErrorMapping.NoError) {
                offsetCommitRequestOpt.foreach(ocr = > offsetManager.putOffsets(ocr.groupId, ocr.requestInfo) )
            }

            val statuses = localProduceResults.map(r = > r.key ->ProducerResponseStatus(r.errorCode, r.start)).toMap;
            val response = offsetCommitRequestOpt.map(_.responseFor(firstErrorCode, config.offsetMetadataMaxSize));
                    .getOrElse(ProducerResponse(produceRequest.correlationId, statuses));

            requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)));
        } else{
            // create a list of (topic, partition) pairs to use as keys for this delayed request;
            val producerRequestKeys = produceRequest.data.keys.toSeq;
            val statuses = localProduceResults.map(r = >
                    r.key ->DelayedProduceResponseStatus(r.end + 1, ProducerResponseStatus(r.errorCode, r.start))).
            toMap;
            val delayedRequest = new DelayedProduce(
                    producerRequestKeys,
                    request,
                    produceRequest.ackTimeoutMs.toLong,
                    produceRequest,
                    statuses,
                    offsetCommitRequestOpt);

            // add the produce request for watch if it's not satisfied, otherwise send the response back;
            val satisfiedByMe = producerRequestPurgatory.checkAndMaybeWatch(delayedRequest);
            if (satisfiedByMe)
                producerRequestPurgatory.respond(delayedRequest);
        }

        // we do not need the data anymore;
        produceRequest.emptyData();
    }

class ProduceResult {
    public TopicAndPartition key;
    public Long start;
    public Long end;
    public Optional<Throwable> error;

    public ProduceResult(TopicAndPartition key, Long start, Long end, Optional<java.lang.Throwable> error) {
        this.key = key;
        this.start = start;
        this.end = end;
        this.error = error;
    }

    public ProduceResult(TopicAndPartition key, Throwable throwable) {
        this(key, -1L, -1L, Optional.of(throwable));
    }


    public Short errorCode() {
        if (error.isPresent()) {
            return ErrorMapping.codeFor(error.getClass());
        } else {
            return ErrorMapping.NoError;
        }
    }
}
//
    /**
     * Helper method for handling a parsed producer request
     */
    private Iterable<ProduceResult> appendToLocalLog(ProducerRequest producerRequest, Boolean isOffsetCommit){
         Map<TopicAndPartition, ByteBufferMessageSet> partitionAndData = producerRequest.data;
        trace(String.format("Append <%s> to local log ",partitionAndData.toString()));
        partitionAndData.forEach((topicAndPartition, messages) ->{
            try {
                if (Topic.InternalTopics.contains(topicAndPartition.topic) &&
                        !(isOffsetCommit && topicAndPartition.topic == OffsetManager.OffsetsTopicName)) {
                    throw new InvalidTopicException(String.format("Cannot append to internal topic %s",topicAndPartition.topic))
                }
                val partitionOpt = replicaManager.getPartition(topicAndPartition.topic, topicAndPartition.partition);
                val info = partitionOpt match {
                    case Some(partition) =>
                        partition.appendMessagesToLeader(messages.asInstanceOf<ByteBufferMessageSet>,producerRequest.requiredAcks);
                    case None => throw new UnknownTopicOrPartitionException("Partition %s doesn't exist on %d";
                            .format(topicAndPartition, brokerId))
                }

                val numAppendedMessages = if (info.firstOffset == -1L || info.lastOffset == -1L) 0 else (info.lastOffset - info.firstOffset + 1)

                // update stats for successfully appended bytes and messages as bytesInRate and messageInRate;
                BrokerTopicStats.getBrokerTopicStats(topicAndPartition.topic).bytesInRate.mark(messages.sizeInBytes);
                BrokerTopicStats.getBrokerAllTopicsStats.bytesInRate.mark(messages.sizeInBytes);
                BrokerTopicStats.getBrokerTopicStats(topicAndPartition.topic).messagesInRate.mark(numAppendedMessages);
                BrokerTopicStats.getBrokerAllTopicsStats.messagesInRate.mark(numAppendedMessages);

                trace("%d bytes written to log %s-%d beginning at offset %d and ending at offset %d";
                        .format(messages.size, topicAndPartition.topic, topicAndPartition.partition, info.firstOffset, info.lastOffset))
                ProduceResult(topicAndPartition, info.firstOffset, info.lastOffset);
            } catch {
            // Failed NOTE produce requests is not incremented for UnknownTopicOrPartitionException and NotLeaderForPartitionException;
            // since failed produce requests metric is supposed to indicate failure of a broker in handling a produce request;
            // for a partition it is the leader for;
            case KafkaStorageException e =>
                fatal("Halting due to unrecoverable I/O error while handling produce request: ", e);
                Runtime.getRuntime.halt(1);
                null;
            case InvalidTopicException ite =>
                warn(String.format("Produce request with correlation id %d from client %s on partition %s failed due to %s",
                        producerRequest.correlationId, producerRequest.clientId, topicAndPartition, ite.getMessage));
                new ProduceResult(topicAndPartition, ite);
            case UnknownTopicOrPartitionException utpe =>
                warn(String.format("Produce request with correlation id %d from client %s on partition %s failed due to %s",
                        producerRequest.correlationId, producerRequest.clientId, topicAndPartition, utpe.getMessage));
                new ProduceResult(topicAndPartition, utpe);
            case NotLeaderForPartitionException nle =>
                warn(String.format("Produce request with correlation id %d from client %s on partition %s failed due to %s",
                        producerRequest.correlationId, producerRequest.clientId, topicAndPartition, nle.getMessage));
                new ProduceResult(topicAndPartition, nle);
            case NotEnoughReplicasException nere =>
                warn(String.format("Produce request with correlation id %d from client %s on partition %s failed due to %s",
                        producerRequest.correlationId, producerRequest.clientId, topicAndPartition, nere.getMessage));
                new ProduceResult(topicAndPartition, nere);
            case Throwable e =>
                BrokerTopicStats.getBrokerTopicStats(topicAndPartition.topic).failedProduceRequestRate.mark();
                BrokerTopicStats.getBrokerAllTopicsStats.failedProduceRequestRate.mark();
                error("Error processing ProducerRequest with correlation id %d from client %s on partition %s";
                        .format(producerRequest.correlationId, producerRequest.clientId, topicAndPartition), e)
                new ProduceResult(topicAndPartition, e);
        }
        }
    }
//
//    /**
//     * Handle a fetch request
//     */
//   public void handleFetchRequest(RequestChannel request.Request) {
//        val fetchRequest = request.requestObj.asInstanceOf<FetchRequest>
//        val dataRead = replicaManager.readMessageSets(fetchRequest);
//
//        // if the fetch request comes from the follower,
//        // update its corresponding log end offset;
//        if(fetchRequest.isFromFollower)
//            recordFollowerLogEndOffsets(fetchRequest.replicaId, dataRead.mapValues(_.offset));
//
//        // check if this fetch request can be satisfied right away;
//        val bytesReadable = dataRead.values.map(_.data.messages.sizeInBytes).sum;
//        val errorReadingData = dataRead.values.foldLeft(false)((errorIncurred, dataAndOffset) =>
//        errorIncurred || (dataAndOffset.data.error != ErrorMapping.NoError));
//        // send the data immediately if 1) fetch request does not want to wait;
//        //                              2) fetch request does not require any data;
//        //                              3) has enough data to respond;
//        //                              4) some error happens while reading data;
//        if(fetchRequest.maxWait <= 0 ||;
//                fetchRequest.numPartitions <= 0 ||;
//                bytesReadable >= fetchRequest.minBytes ||;
//                errorReadingData) {
//            debug("Returning fetch response %s for fetch request with correlation id %d to client %s";
//                    .format(dataRead.values.map(_.data.error).mkString(","), fetchRequest.correlationId, fetchRequest.clientId))
//            val response = new FetchResponse(fetchRequest.correlationId, dataRead.mapValues(_.data));
//            requestChannel.sendResponse(new RequestChannel.Response(request, new FetchResponseSend(response)));
//        } else {
//            debug(String.format("Putting fetch request with correlation id %d from client %s into purgatory",fetchRequest.correlationId,
//                    fetchRequest.clientId));
//            // create a list of (topic, partition) pairs to use as keys for this delayed request;
//            val delayedFetchKeys = fetchRequest.requestInfo.keys.toSeq;
//            val delayedFetch = new DelayedFetch(delayedFetchKeys, request, fetchRequest.maxWait, fetchRequest,
//                    dataRead.mapValues(_.offset));
//
//            // add the fetch request for watch if it's not satisfied, otherwise send the response back;
//            val satisfiedByMe = fetchRequestPurgatory.checkAndMaybeWatch(delayedFetch);
//            if (satisfiedByMe)
//                fetchRequestPurgatory.respond(delayedFetch);
//        }
//    }
//
//    privatepublic void recordFollowerLogEndOffsets(Int replicaId, Map offsets<TopicAndPartition, LogOffsetMetadata>) {
//        debug(String.format("Record follower log end offsets: %s ",offsets))
//        offsets.foreach {
//            case (topicAndPartition, offset) =>
//                replicaManager.updateReplicaLEOAndPartitionHW(topicAndPartition.topic,
//                        topicAndPartition.partition, replicaId, offset);
//
//                // for producer requests with ack > 1, we need to check;
//                // if they can be unblocked after some follower's log end offsets have moved;
//                replicaManager.unblockDelayedProduceRequests(topicAndPartition);
//        }
//    }
//
//    /**
//     * Service the offset request API
//     */
//   public void handleOffsetRequest(RequestChannel request.Request) {
//        val offsetRequest = request.requestObj.asInstanceOf<OffsetRequest>
//        val responseMap = offsetRequest.requestInfo.map(elem => {
//                val (topicAndPartition, partitionOffsetRequestInfo) = elem;
//        try {
//            // ensure leader exists;
//            val localReplica = if(!offsetRequest.isFromDebuggingClient)
//                replicaManager.getLeaderReplicaIfLocal(topicAndPartition.topic, topicAndPartition.partition);
//            else;
//                replicaManager.getReplicaOrException(topicAndPartition.topic, topicAndPartition.partition);
//            val offsets = {
//                    val allOffsets = fetchOffsets(replicaManager.logManager,
//                    topicAndPartition,
//                    partitionOffsetRequestInfo.time,
//                    partitionOffsetRequestInfo.maxNumOffsets);
//            if (!offsetRequest.isFromOrdinaryClient) {
//                allOffsets;
//            } else {
//                val hw = localReplica.highWatermark.messageOffset;
//                if (allOffsets.exists(_ > hw))
//                    hw +: allOffsets.dropWhile(_ > hw);
//            else;
//                allOffsets;
//            }
//        }
//            (topicAndPartition, PartitionOffsetsResponse(ErrorMapping.NoError, offsets));
//        } catch {
//            // UnknownTopicOrPartitionException NOTE and NotLeaderForPartitionException are special cased since these error messages;
//            // are typically transient and there is no value in logging the entire stack trace for the same;
//            case UnknownTopicOrPartitionException utpe =>
//                warn(String.format("Offset request with correlation id %d from client %s on partition %s failed due to %s",
//                        offsetRequest.correlationId, offsetRequest.clientId, topicAndPartition, utpe.getMessage));
//                (topicAndPartition, PartitionOffsetsResponse(ErrorMapping.codeFor(utpe.getClass.asInstanceOf<Class<Throwable>>), Nil) );
//            case NotLeaderForPartitionException nle =>
//                warn(String.format("Offset request with correlation id %d from client %s on partition %s failed due to %s",
//                        offsetRequest.correlationId, offsetRequest.clientId, topicAndPartition,nle.getMessage));
//                (topicAndPartition, PartitionOffsetsResponse(ErrorMapping.codeFor(nle.getClass.asInstanceOf<Class<Throwable>>), Nil) );
//            case Throwable e =>
//                warn("Error while responding to offset request", e);
//                (topicAndPartition, PartitionOffsetsResponse(ErrorMapping.codeFor(e.getClass.asInstanceOf<Class<Throwable>>), Nil) );
//        }
//    });
//        val response = OffsetResponse(offsetRequest.correlationId, responseMap);
//        requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)));
//    }
//
//   public void fetchOffsets(LogManager logManager, TopicAndPartition topicAndPartition, Long timestamp, Int maxNumOffsets): Seq<Long> = {
//        logManager.getLog(topicAndPartition) match {
//            case Some(log) =>
//                fetchOffsetsBefore(log, timestamp, maxNumOffsets)
//            case None =>
//                if (timestamp == OffsetRequest.LatestTime || timestamp == OffsetRequest.EarliestTime)
//                    Seq(0L);
//                else;
//                    Nil;
//        }
//    }
//
//   public void fetchOffsetsBefore(Log log, Long timestamp, Int maxNumOffsets): Seq<Long> = {
//        val segsArray = log.logSegments.toArray;
//        var Array offsetTimeArray<(Long, Long)> = null;
//        if(segsArray.last.size > 0)
//            offsetTimeArray = new Array<(Long, Long)>(segsArray.length + 1);
//    else;
//        offsetTimeArray = new Array<(Long, Long)>(segsArray.length);
//
//        for(i <- 0 until segsArray.length)
//        offsetTimeArray(i) = (segsArray(i).baseOffset, segsArray(i).lastModified)
//        if(segsArray.last.size > 0)
//            offsetTimeArray(segsArray.length) = (log.logEndOffset, SystemTime.milliseconds);
//
//        var startIndex = -1;
//        timestamp match {
//            case OffsetRequest.LatestTime =>
//                startIndex = offsetTimeArray.length - 1;
//            case OffsetRequest.EarliestTime =>
//                startIndex = 0;
//            case _ =>
//                var isFound = false;
//                debug(String.format("Offset time array = " + offsetTimeArray.foreach(o => "%d, %d",o._1, o._2)))
//                startIndex = offsetTimeArray.length - 1;
//                while (startIndex >= 0 && !isFound) {
//                    if (offsetTimeArray(startIndex)._2 <= timestamp)
//                        isFound = true;
//                    else;
//                        startIndex -=1;
//                }
//        }
//
//        val retSize = maxNumOffsets.min(startIndex + 1);
//        val ret = new Array<Long>(retSize);
//        for(j <- 0 until retSize) {
//            ret(j) = offsetTimeArray(startIndex)._1;
//            startIndex -= 1;
//        }
//        // ensure that the returned seq is in descending order of offsets;
//        ret.toSeq.sortBy(- _);
//    }
//
//    privatepublic void getTopicMetadata(Set topics<String]): Seq[TopicMetadata> = {
//        val topicResponses = metadataCache.getTopicMetadata(topics);
//        if (topics.size > 0 && topicResponses.size != topics.size) {
//            val nonExistentTopics = topics -- topicResponses.map(_.topic).toSet;
//            val responsesForNonExistentTopics = nonExistentTopics.map { topic =>
//                if (topic == OffsetManager.OffsetsTopicName || config.autoCreateTopicsEnable) {
//                    try {
//                        if (topic == OffsetManager.OffsetsTopicName) {
//                            val aliveBrokers = metadataCache.getAliveBrokers;
//                            val offsetsTopicReplicationFactor =
//                            if (aliveBrokers.length > 0)
//                                Math.min(config.offsetsTopicReplicationFactor, aliveBrokers.length);
//                            else;
//                                config.offsetsTopicReplicationFactor;
//                            AdminUtils.createTopic(zkClient, topic, config.offsetsTopicPartitions,
//                                    offsetsTopicReplicationFactor,
//                                    offsetManager.offsetsTopicConfig);
//                            info("Auto creation of topic %s with %d partitions and replication factor %d is successful!";
//                                    .format(topic, config.offsetsTopicPartitions, offsetsTopicReplicationFactor))
//                        }
//                        else {
//                            AdminUtils.createTopic(zkClient, topic, config.numPartitions, config.defaultReplicationFactor);
//                            info("Auto creation of topic %s with %d partitions and replication factor %d is successful!";
//                                    .format(topic, config.numPartitions, config.defaultReplicationFactor))
//                        }
//                    } catch {
//                        case TopicExistsException e => // let it go, possibly another broker created this topic;
//                    }
//                    new TopicMetadata(topic, Seq.empty<PartitionMetadata>, ErrorMapping.LeaderNotAvailableCode);
//                } else {
//                    new TopicMetadata(topic, Seq.empty<PartitionMetadata>, ErrorMapping.UnknownTopicOrPartitionCode);
//                }
//            }
//            topicResponses.appendAll(responsesForNonExistentTopics);
//        }
//        topicResponses;
//    }
//
//    /**
//     * Service the topic metadata request API
//     */
//   public void handleTopicMetadataRequest(RequestChannel request.Request) {
//        val metadataRequest = request.requestObj.asInstanceOf<TopicMetadataRequest>
//        val topicMetadata = getTopicMetadata(metadataRequest.topics.toSet);
//        val brokers = metadataCache.getAliveBrokers;
//        trace(String.format("Sending topic metadata %s and brokers %s for correlation id %d to client %s",topicMetadata.mkString(","), brokers.mkString(","), metadataRequest.correlationId, metadataRequest.clientId))
//        val response = new TopicMetadataResponse(brokers, topicMetadata, metadataRequest.correlationId);
//        requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)));
//    }
//
//    /*
//     * Service the Offset fetch API
//     */
//   public void handleOffsetFetchRequest(RequestChannel request.Request) {
//        val offsetFetchRequest = request.requestObj.asInstanceOf<OffsetFetchRequest>
//
//        if (offsetFetchRequest.versionId == 0) {
//            // version 0 reads offsets from ZK;
//            val responseInfo = offsetFetchRequest.requestInfo.map( t => {
//                    val topicDirs = new ZKGroupTopicDirs(offsetFetchRequest.groupId, t.topic);
//            try {
//                ensureTopicExists(t.topic);
//                val payloadOpt = ZkUtils.readDataMaybeNull(zkClient, topicDirs.consumerOffsetDir + "/" + t.partition)._1;
//                payloadOpt match {
//                    case Some(payload) => {
//                        (t, OffsetMetadataAndError(offset=payload.toLong, error=ErrorMapping.NoError));
//                    }
//                    case None => (t, OffsetMetadataAndError(OffsetAndMetadata.InvalidOffset, OffsetAndMetadata.NoMetadata,
//                            ErrorMapping.UnknownTopicOrPartitionCode));
//                }
//            } catch {
//                case Throwable e =>
//                    (t, OffsetMetadataAndError(OffsetAndMetadata.InvalidOffset, OffsetAndMetadata.NoMetadata,
//                            ErrorMapping.codeFor(e.getClass.asInstanceOf<Class<Throwable>>)));
//            }
//      });
//            val response = new OffsetFetchResponse(collection.immutable.Map(_ responseInfo*), offsetFetchRequest.correlationId);
//            requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)));
//        } else {
//            // version 1 reads offsets from Kafka;
//            val (unknownTopicPartitions, knownTopicPartitions) = offsetFetchRequest.requestInfo.partition(topicAndPartition =>
//                    metadataCache.getPartitionInfo(topicAndPartition.topic, topicAndPartition.partition).isEmpty;
//      );
//            val unknownStatus = unknownTopicPartitions.map(topicAndPartition => (topicAndPartition, OffsetMetadataAndError.UnknownTopicOrPartition)).toMap;
//            val knownStatus =
//            if (knownTopicPartitions.size > 0)
//                offsetManager.getOffsets(offsetFetchRequest.groupId, knownTopicPartitions).toMap;
//            else;
//                Map.empty<TopicAndPartition, OffsetMetadataAndError>
//            val status = unknownStatus ++ knownStatus;
//
//            val response = OffsetFetchResponse(status, offsetFetchRequest.correlationId);
//
//            trace("Sending offset fetch response %s for correlation id %d to client %s.";
//                    .format(response, offsetFetchRequest.correlationId, offsetFetchRequest.clientId))
//            requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)));
//        }
//    }
//
//    /*
//     * Service the consumer metadata API
//     */
//   public void handleConsumerMetadataRequest(RequestChannel request.Request) {
//        val consumerMetadataRequest = request.requestObj.asInstanceOf<ConsumerMetadataRequest>
//
//        val partition = offsetManager.partitionFor(consumerMetadataRequest.group);
//
//        // get metadata (and create the topic if necessary)
//        val offsetsTopicMetadata = getTopicMetadata(Set(OffsetManager.OffsetsTopicName)).head;
//
//        val errorResponse = ConsumerMetadataResponse(None, ErrorMapping.ConsumerCoordinatorNotAvailableCode, consumerMetadataRequest.correlationId);
//
//        val response =
//                offsetsTopicMetadata.partitionsMetadata.find(_.partitionId == partition).map { partitionMetadata =>
//            partitionMetadata.leader.map { leader =>
//                ConsumerMetadataResponse(Some(leader), ErrorMapping.NoError, consumerMetadataRequest.correlationId);
//            }.getOrElse(errorResponse);
//        }.getOrElse(errorResponse);
//
//        trace("Sending consumer metadata %s for correlation id %d to client %s.";
//                .format(response, consumerMetadataRequest.correlationId, consumerMetadataRequest.clientId))
//        requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)));
//    }
//
//   public void close() {
//        debug("Shutting down.");
//        fetchRequestPurgatory.shutdown();
//        producerRequestPurgatory.shutdown();
//        debug("Shut down complete.");
//    }

}
