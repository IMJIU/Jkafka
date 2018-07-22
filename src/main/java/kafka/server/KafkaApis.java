package kafka.server;/**
 * Created by zhoulf on 2017/5/11.
 */

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import kafka.admin.AdminUtils;
import kafka.api.*;
import kafka.cluster.Broker;
import kafka.cluster.Partition;
import kafka.cluster.Replica;
import kafka.common.*;
import kafka.controller.KafkaController;
import kafka.func.Tuple;
import kafka.log.*;
import kafka.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.network.BoundedByteBufferSend;
import kafka.network.RequestChannel;
import kafka.utils.*;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.apache.kafka.common.errors.NotLeaderForPartitionException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Logic to handle the various Kafka requests
 */
public class KafkaApis extends Logging {


    public RequestChannel requestChannel;
    public ReplicaManager replicaManager;
    public OffsetManager offsetManager;
    public ZkClient zkClient;
    public Integer brokerId;
    public KafkaConfig config;
    public KafkaController controller;
    public MetadataCache metadataCache = new MetadataCache();
    public ProducerRequestPurgatory producerRequestPurgatory;
    public FetchRequestPurgatory fetchRequestPurgatory;

    public KafkaApis(RequestChannel requestChannel, ReplicaManager replicaManager, OffsetManager offsetManager, ZkClient zkClient, Integer brokerId, KafkaConfig config, KafkaController controller) {
        this.requestChannel = requestChannel;
        this.replicaManager = replicaManager;
        this.offsetManager = offsetManager;
        this.zkClient = zkClient;
        this.brokerId = brokerId;
        this.config = config;
        this.controller = controller;
        producerRequestPurgatory = new ProducerRequestPurgatory(replicaManager, offsetManager, requestChannel);
        fetchRequestPurgatory = new FetchRequestPurgatory(replicaManager, requestChannel);
        // the TODO following line will be removed in 0.9;
        replicaManager.initWithRequestPurgatory(producerRequestPurgatory, fetchRequestPurgatory);
        loggerName(String.format("<KafkaApi-%d> ", brokerId));
    }

    /**
     * Top-level method that handles all requests and multiplexes to the right api
     */
    public void handle(RequestChannel.Request request) {
        try {
            trace("Handling request: " + request.requestObj + " from client: " + request.remoteAddress);
            switch (request.requestId) {
                case RequestKeys.ProduceKey:
                    handleProducerOrOffsetCommitRequest(request);
                    break;
                case RequestKeys.FetchKey:
                    handleFetchRequest(request);
                    break;
                case RequestKeys.OffsetsKey:
                    handleOffsetRequest(request);
                    break;
                case RequestKeys.MetadataKey:
                    handleTopicMetadataRequest(request);
                    break;
                case RequestKeys.LeaderAndIsrKey:
                    handleLeaderAndIsrRequest(request);
                    break;
                case RequestKeys.StopReplicaKey:
                    handleStopReplicaRequest(request);
                    break;
                case RequestKeys.UpdateMetadataKey:
                    handleUpdateMetadataRequest(request);
                    break;
                case RequestKeys.ControlledShutdownKey:
                    handleControlledShutdownRequest(request);
                    break;
                case RequestKeys.OffsetCommitKey:
                    handleOffsetCommitRequest(request);
                    break;
                case RequestKeys.OffsetFetchKey:
                    handleOffsetFetchRequest(request);
                    break;
                case RequestKeys.ConsumerMetadataKey:
                    handleConsumerMetadataRequest(request);
                    break;
                default:
                    throw new KafkaException("Unknown api code " + request.requestId);
            }
        } catch (Throwable e) {
            e.printStackTrace();
            request.requestObj.handleError(e, requestChannel, request);
            error(String.format("error when handling request %s", request.requestObj), e);
        } finally {
            request.apiLocalCompleteTimeMs = Time.get().milliseconds();
        }
    }


    public void handleOffsetCommitRequest(RequestChannel.Request request) {
        OffsetCommitRequest offsetCommitRequest = (OffsetCommitRequest) request.requestObj;
        if (offsetCommitRequest.versionId == 0) {
            // version 0 stores the offsets in ZK;
            Map<TopicAndPartition, Short> responseInfo = Sc.toMap(Sc.map(offsetCommitRequest.requestInfo, (topicAndPartition, metaAndError) -> {
                ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs(offsetCommitRequest.groupId, topicAndPartition.topic);
                try {
                    ensureTopicExists(topicAndPartition.topic);
                    if (metaAndError.metadata != null && metaAndError.metadata.length() > config.offsetMetadataMaxSize) {
                        return Tuple.of(topicAndPartition, ErrorMapping.OffsetMetadataTooLargeCode);
                    } else {
                        ZkUtils.updatePersistentPath(zkClient, topicDirs.consumerOffsetDir() + "/" +
                                topicAndPartition.partition, metaAndError.offset.toString());
                        return Tuple.of(topicAndPartition, ErrorMapping.NoError);
                    }
                } catch (Throwable e) {
                    return Tuple.of(topicAndPartition, ErrorMapping.codeFor(e.getClass()));
                }
            }));
            OffsetCommitResponse response = new OffsetCommitResponse(responseInfo, offsetCommitRequest.correlationId);
            requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)));
        } else {
            // version 1 and above store the offsets in a special Kafka topic;
            handleProducerOrOffsetCommitRequest(request);
        }
    }

    private void ensureTopicExists(String topic) {
        if (metadataCache.getTopicMetadata(Sets.newHashSet(topic)).size() <= 0)
            throw new UnknownTopicOrPartitionException("Topic " + topic + " either doesn't exist or is in the process of being deleted");
    }

    public void handleLeaderAndIsrRequest(RequestChannel.Request request) {
        // ensureTopicExists is only for client facing requests;
        // We can't have the ensureTopicExists check here since the controller sends it as an advisory to all brokers so they;
        // stop serving data to clients for the topic being deleted;
        LeaderAndIsrRequest leaderAndIsrRequest = (LeaderAndIsrRequest) request.requestObj;
        try {
            Tuple<Map<Tuple<String, Integer>, Short>, Short> tuple = replicaManager.becomeLeaderOrFollower(leaderAndIsrRequest, offsetManager);
            Map<Tuple<String, Integer>, Short> response = tuple.v1;
            Short error = tuple.v2;
            LeaderAndIsrResponse leaderAndIsrResponse = new LeaderAndIsrResponse(leaderAndIsrRequest.correlationId, response, error);
            requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(leaderAndIsrResponse)));
        } catch (KafkaStorageException e) {
            error("Disk error during leadership change.", e);
            Runtime.getRuntime().halt(1);
        }
    }

    public void handleStopReplicaRequest(RequestChannel.Request request) {
        // ensureTopicExists is only for client facing requests;
        // We can't have the ensureTopicExists check here since the controller sends it as an advisory to all brokers so they;
        // stop serving data to clients for the topic being deleted;
        StopReplicaRequest stopReplicaRequest = (StopReplicaRequest) request.requestObj;
        Tuple<Map<TopicAndPartition, Short>, Short> tuple = replicaManager.stopReplicas(stopReplicaRequest);
        Map<TopicAndPartition, Short> response = tuple.v1;
        Short error = tuple.v2;
        StopReplicaResponse stopReplicaResponse = new StopReplicaResponse(stopReplicaRequest.correlationId, response, error);
        requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(stopReplicaResponse)));
        replicaManager.replicaFetcherManager.shutdownIdleFetcherThreads();
    }

    public void handleUpdateMetadataRequest(RequestChannel.Request request) {
        UpdateMetadataRequest updateMetadataRequest = (UpdateMetadataRequest) request.requestObj;
        replicaManager.maybeUpdateMetadataCache(updateMetadataRequest, metadataCache);

        UpdateMetadataResponse updateMetadataResponse = new UpdateMetadataResponse(updateMetadataRequest.correlationId, ErrorMapping.NoError);
        requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(updateMetadataResponse)));
    }

    public void handleControlledShutdownRequest(RequestChannel.Request request) {
        // ensureTopicExists is only for client facing requests;
        // We can't have the ensureTopicExists check here since the controller sends it as an advisory to all brokers so they;
        // stop serving data to clients for the topic being deleted;
//        ControlledShutdownRequest controlledShutdownRequest = request.requestObj;
//                val partitionsRemaining = controller.shutdownBroker(controlledShutdownRequest.brokerId);
//        val controlledShutdownResponse = new ControlledShutdownResponse(controlledShutdownRequest.correlationId,
//                ErrorMapping.NoError, partitionsRemaining);
//        requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(controlledShutdownResponse)));
    }

    private ProducerRequest producerRequestFromOffsetCommit(OffsetCommitRequest offsetCommitRequest) {
        List<Message> msgs = offsetCommitRequest.filterLargeMetadata(config.offsetMetadataMaxSize).map(entry -> {
            TopicAndPartition topicAndPartition = entry.getKey();
            OffsetAndMetadata offset = entry.getValue();
            return new Message(OffsetManager.offsetCommitValue(offset),
                    OffsetManager.offsetCommitKey(offsetCommitRequest.groupId, topicAndPartition.topic, topicAndPartition.partition));
        }).collect(Collectors.toList());

        Map<TopicAndPartition, ByteBufferMessageSet> producerData = Sc.toMap(
                new TopicAndPartition(OffsetManager.OffsetsTopicName, offsetManager.partitionFor(offsetCommitRequest.groupId)),
                new ByteBufferMessageSet(config.offsetsTopicCompressionCodec, msgs)
        );

        ProducerRequest request = new ProducerRequest(
                offsetCommitRequest.correlationId,
                offsetCommitRequest.clientId,
                config.offsetCommitRequiredAcks,
                config.offsetCommitTimeoutMs,
                producerData);
        trace(String.format("Created producer request %s for offset commit request %s.", request, offsetCommitRequest));
        return request;
    }

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
                    "and recommended configuration.").format(produceRequest.clientId, request.remoteAddress, produceRequest.requiredAcks));
        }

        Long sTime = Time.get().milliseconds();
        Iterable<ProduceResult> localProduceResults = appendToLocalLog(produceRequest, offsetCommitRequestOpt.isPresent());
        debug(String.format("Produce to local log in %d ms", Time.get().milliseconds() - sTime));
        ProduceResult produceResult = Sc.find(localProduceResults, r -> r.errorCode() != ErrorMapping.NoError);
        Short firstErrorCode;
        if (produceResult != null) {
            firstErrorCode = produceResult.errorCode();
        } else {
            firstErrorCode = ErrorMapping.NoError;
        }
        int numPartitionsInError = Sc.count(localProduceResults, r -> r.error.isPresent());
        if (produceRequest.requiredAcks == 0) {
            // no operation needed if producer request.required.acks = 0; however, if there is any exception in handling the request, since;
            // no response is expected by the producer the handler will send a close connection response to the socket server;
            // to close the socket so that the producer client will know that some exception has happened and will refresh its metadata;
            if (numPartitionsInError != 0) {
                info(String.format("Send the close connection response due to error handling produce request " +
                                "<clientId = %s, correlationId = %s, topicAndPartition = %s> with Ack=0",
                        produceRequest.clientId, produceRequest.correlationId, produceRequest.topicPartitionMessageSizeMap.keySet()));
                requestChannel.closeConnection(request.processor, request);
            } else {
                if (firstErrorCode == ErrorMapping.NoError)
                    if (offsetCommitRequestOpt.isPresent()) {
                        offsetManager.putOffsets(offsetCommitRequestOpt.get().groupId, offsetCommitRequestOpt.get().requestInfo);
                    }

                if (offsetCommitRequestOpt.isPresent()) {
                    OffsetCommitResponse response = offsetCommitRequestOpt.get().responseFor(firstErrorCode, config.offsetMetadataMaxSize);
                    requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)));
                } else
                    requestChannel.noOperation(request.processor, request);
            }
        } else if (produceRequest.requiredAcks == 1 ||
                produceRequest.numPartitions() <= 0 ||
                numPartitionsInError == produceRequest.numPartitions()) {

            if (firstErrorCode == ErrorMapping.NoError) {
                if (offsetCommitRequestOpt.isPresent()) {
                    offsetManager.putOffsets(offsetCommitRequestOpt.get().groupId, offsetCommitRequestOpt.get().requestInfo);
                }
            }
            Map<TopicAndPartition, ProducerResponseStatus> statuses = Sc.toMap(Sc.map(localProduceResults, r -> Tuple.of(r.key, new ProducerResponseStatus(r.errorCode(), r.start))));
            final Short errorCode = firstErrorCode;
            RequestOrResponse response = Sc.getOrElse(Sc.map(offsetCommitRequestOpt, r -> r.responseFor(errorCode, config.offsetMetadataMaxSize)), new ProducerResponse(produceRequest.correlationId, statuses));
            requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)));
        } else {
            // create a list of (topic, partition) pairs to use as keys for this delayed request;
            List<TopicAndPartition> producerRequestKeys = Sc.toList(produceRequest.data.keySet());
            Map<TopicAndPartition, DelayedProduceResponseStatus> statuses = Sc.toMap(Sc.map(localProduceResults, r ->
                    Tuple.of(r.key, new DelayedProduceResponseStatus(r.end + 1, new ProducerResponseStatus(r.errorCode(), r.start)))));
            DelayedProduce delayedRequest = new DelayedProduce(
                    producerRequestKeys,
                    request,
                    produceRequest.ackTimeoutMs.longValue(),
                    produceRequest,
                    statuses,
                    offsetCommitRequestOpt);

            // add the produce request for watch if it's not satisfied, otherwise send the response back;
            Boolean satisfiedByMe = producerRequestPurgatory.checkAndMaybeWatch(delayedRequest);
           logger.info("===satisfied:" + satisfiedByMe);
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

        public ProduceResult(TopicAndPartition key, Long start, Long end) {
            this(key, start, end, Optional.empty());
        }

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
    private Iterable<ProduceResult> appendToLocalLog(ProducerRequest producerRequest, Boolean isOffsetCommit) {
        Map<TopicAndPartition, ByteBufferMessageSet> partitionAndData = producerRequest.data;
        trace(String.format("Append <%s> to local log ", partitionAndData.toString()));
        return Sc.map(partitionAndData, (topicAndPartition, messages) -> {
            try {
                if (Topic.InternalTopics.contains(topicAndPartition.topic) &&
                        !(isOffsetCommit && topicAndPartition.topic == OffsetManager.OffsetsTopicName)) {
                    throw new InvalidTopicException(String.format("Cannot append to internal topic %s", topicAndPartition.topic));
                }
                Optional<Partition> partitionOpt = replicaManager.getPartition(topicAndPartition.topic, topicAndPartition.partition);
                LogAppendInfo info;
                if (partitionOpt.isPresent()) {
                    info = partitionOpt.get().appendMessagesToLeader(messages, producerRequest.requiredAcks.intValue());
                } else {
                    throw new UnknownTopicOrPartitionException(String.format("Partition %s doesn't exist on %d", topicAndPartition, brokerId));
                }


                long numAppendedMessages = (info.firstOffset == -1L || info.lastOffset == -1L) ? 0 : (info.lastOffset - info.firstOffset + 1);

                // update stats for successfully appended bytes and messages as bytesInRate and messageInRate;
                BrokerTopicStats.getBrokerTopicStats(topicAndPartition.topic).bytesInRate.mark(messages.sizeInBytes());
                BrokerTopicStats.getBrokerAllTopicsStats().bytesInRate.mark(messages.sizeInBytes());
                BrokerTopicStats.getBrokerTopicStats(topicAndPartition.topic).messagesInRate.mark(numAppendedMessages);
                BrokerTopicStats.getBrokerAllTopicsStats().messagesInRate.mark(numAppendedMessages);

                trace(String.format("%d bytes written to log %s-%d beginning at offset %d and ending at offset %d",
                        messages.sizeInBytes(), topicAndPartition.topic, topicAndPartition.partition, info.firstOffset, info.lastOffset));
                return new ProduceResult(topicAndPartition, info.firstOffset, info.lastOffset);
            } catch (KafkaStorageException e) {
                // Failed NOTE produce requests is not incremented for UnknownTopicOrPartitionException and NotLeaderForPartitionException;
                // since failed produce requests metric is supposed to indicate failure of a broker in handling a produce request;
                // for a partition it is the leader for;
                error("Halting due to unrecoverable I/O error while handling produce request: ", e);
                Runtime.getRuntime().halt(1);
                return null;
            } catch (InvalidTopicException ite) {
                warn(String.format("Produce request with correlation id %d from client %s on partition %s failed due to %s",
                        producerRequest.correlationId, producerRequest.clientId, topicAndPartition, ite.getMessage()));
                return new ProduceResult(topicAndPartition, ite);
            } catch (UnknownTopicOrPartitionException utpe) {
                warn(String.format("Produce request with correlation id %d from client %s on partition %s failed due to %s",
                        producerRequest.correlationId, producerRequest.clientId, topicAndPartition, utpe.getMessage()));
                return new ProduceResult(topicAndPartition, utpe);
            } catch (NotLeaderForPartitionException nle) {
                warn(String.format("Produce request with correlation id %d from client %s on partition %s failed due to %s",
                        producerRequest.correlationId, producerRequest.clientId, topicAndPartition, nle.getMessage()));
                return new ProduceResult(topicAndPartition, nle);
            } catch (NotEnoughReplicasException nere) {
                warn(String.format("Produce request with correlation id %d from client %s on partition %s failed due to %s",
                        producerRequest.correlationId, producerRequest.clientId, topicAndPartition, nere.getMessage()));
                return new ProduceResult(topicAndPartition, nere);
            } catch (Throwable e) {
                BrokerTopicStats.getBrokerTopicStats(topicAndPartition.topic).failedProduceRequestRate.mark();
                BrokerTopicStats.getBrokerAllTopicsStats().failedProduceRequestRate.mark();
                error(String.format("Error processing ProducerRequest with correlation id %d from client %s on partition %s"
                        , producerRequest.correlationId, producerRequest.clientId, topicAndPartition), e);
                return new ProduceResult(topicAndPartition, e);
            }
        });
    }

    /**
     * Handle a fetch request
     */
    public void handleFetchRequest(RequestChannel.Request request) {
        FetchRequest fetchRequest = (FetchRequest) request.requestObj;
        Map<TopicAndPartition, PartitionDataAndOffset> dataRead = replicaManager.readMessageSets(fetchRequest);

        // if the fetch request comes from the follower,
        // update its corresponding log end offset;
        if (fetchRequest.isFromFollower())
            recordFollowerLogEndOffsets(fetchRequest.replicaId, Sc.mapValue(dataRead, v -> v.offset));

        // check if this fetch request can be satisfied right away;

        int bytesReadable = Sc.sum(dataRead.values(), v -> v.data.messages.sizeInBytes());
        boolean errorReadingData = Sc.foldBooleanOr(dataRead.values(), false, dataAndOffset -> (dataAndOffset.data.error != ErrorMapping.NoError));
        // send the data immediately if 1) fetch request does not want to wait;
        //                              2) fetch request does not require any data;
        //                              3) has enough data to respond;
        //                              4) some error happens while reading data;
        if (fetchRequest.maxWait <= 0 ||
                fetchRequest.numPartitions() <= 0 ||
                bytesReadable >= fetchRequest.minBytes ||
                errorReadingData) {
            debug(String.format("Returning fetch response %s for fetch request with correlation id %d to client %s",
                    Sc.map(dataRead.values(), d -> d.data.error), fetchRequest.correlationId, fetchRequest.clientId));
            FetchResponse response = new FetchResponse(fetchRequest.correlationId, Sc.mapValue(dataRead, v -> v.data));
            requestChannel.sendResponse(new RequestChannel.Response(request, new FetchResponseSend(response)));
        } else {
            debug(String.format("Putting fetch request with correlation id %d from client %s into purgatory", fetchRequest.correlationId,
                    fetchRequest.clientId));
            // create a list of (topic, partition) pairs to use as keys for this delayed request;
            List<TopicAndPartition> delayedFetchKeys = Sc.toList(fetchRequest.requestInfo.keySet());
            DelayedFetch delayedFetch = new DelayedFetch(delayedFetchKeys, request, fetchRequest.maxWait.longValue(), fetchRequest,
                    Sc.mapValue(dataRead, v -> v.offset));

            // add the fetch request for watch if it's not satisfied, otherwise send the response back;
            boolean satisfiedByMe = fetchRequestPurgatory.checkAndMaybeWatch(delayedFetch);
            if (satisfiedByMe)
                fetchRequestPurgatory.respond(delayedFetch);
        }
    }

    private void recordFollowerLogEndOffsets(Integer replicaId, Map<TopicAndPartition, LogOffsetMetadata> offsets) {
        debug(String.format("Record follower log end offsets: %s ", offsets));
        offsets.forEach((topicAndPartition, offset) -> {
            replicaManager.updateReplicaLEOAndPartitionHW(topicAndPartition.topic,
                    topicAndPartition.partition, replicaId, offset);

            // for producer requests with ack > 1, we need to check;
            // if they can be unblocked after some follower's log end offsets have moved;
            replicaManager.unblockDelayedProduceRequests(topicAndPartition);
        });
    }

    /**
     * Service the offset request API
     */
    public void handleOffsetRequest(RequestChannel.Request request) {
        OffsetRequest offsetRequest = (OffsetRequest) request.requestObj;
        Map<TopicAndPartition, PartitionOffsetsResponse> responseMap = Sc.toMap(Sc.map(offsetRequest.requestInfo, (topicAndPartition, partitionOffsetRequestInfo) -> {
            try {
                // ensure leader exists;
                Replica localReplica = (!offsetRequest.isFromDebuggingClient()) ?
                        replicaManager.getLeaderReplicaIfLocal(topicAndPartition.topic, topicAndPartition.partition) : replicaManager.getReplicaOrException(topicAndPartition.topic, topicAndPartition.partition);
                List<Long> offsets;
                List<Long> allOffsets = fetchOffsets(replicaManager.logManager,
                        topicAndPartition,
                        partitionOffsetRequestInfo.time,
                        partitionOffsetRequestInfo.maxNumOffsets);
                if (!offsetRequest.isFromOrdinaryClient()) {
                    offsets = allOffsets;
                } else {
                    long hw = localReplica.highWatermark().messageOffset;
                    if (Sc.exists(allOffsets, f -> f > hw)) {
                        offsets = Lists.newArrayList();
                        offsets.addAll(Sc.filter(allOffsets, n -> n <= hw));
                    } else {
                        offsets = allOffsets;
                    }
                }
                return Tuple.of(topicAndPartition, new PartitionOffsetsResponse(ErrorMapping.NoError, offsets));
            } catch (UnknownTopicOrPartitionException utpe) {
                // UnknownTopicOrPartitionException NOTE and NotLeaderForPartitionException are special cased since these error messages;
                // are typically transient and there is no value in logging the entire stack trace for the same;

                warn(String.format("Offset request with correlation id %d from client %s on partition %s failed due to %s",
                        offsetRequest.correlationId, offsetRequest.clientId, topicAndPartition, utpe.getMessage()));
                return Tuple.of(topicAndPartition, new PartitionOffsetsResponse(ErrorMapping.codeFor(utpe.getClass()), Collections.emptyList()));
            } catch (NotLeaderForPartitionException nle) {
                warn(String.format("Offset request with correlation id %d from client %s on partition %s failed due to %s",
                        offsetRequest.correlationId, offsetRequest.clientId, topicAndPartition, nle.getMessage()));
                return Tuple.of(topicAndPartition, new PartitionOffsetsResponse(ErrorMapping.codeFor(nle.getClass()), Collections.emptyList()));
            } catch (Throwable e) {
                warn("Error while responding to offset request", e);
                return Tuple.of(topicAndPartition, new PartitionOffsetsResponse(ErrorMapping.codeFor(e.getClass()), Collections.emptyList()));
            }
        }));
        OffsetResponse response = new OffsetResponse(offsetRequest.correlationId, responseMap);
        requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)));
    }

    public List<Long> fetchOffsets(LogManager logManager, TopicAndPartition topicAndPartition, Long timestamp, Integer maxNumOffsets) {
        return Sc.match(logManager.getLog(topicAndPartition), (log) ->
                        fetchOffsetsBefore(log, timestamp, maxNumOffsets)
                , () -> (timestamp == OffsetRequest.LatestTime || timestamp == OffsetRequest.EarliestTime) ?
                        Lists.newArrayList(0L) : null);
    }

    public List<Long> fetchOffsetsBefore(Log log, Long timestamp, Integer maxNumOffsets) {
        LogSegment[] segsArray = log.logSegments().toArray(new LogSegment[log.logSegments().size()]);
        Tuple<Long, Long>[] offsetTimeArray;
        if (segsArray[segsArray.length - 1].size() > 0)
            offsetTimeArray = new Tuple[segsArray.length + 1];
        else
            offsetTimeArray = new Tuple[segsArray.length];

        for (int i = 0; i < segsArray.length; i++)
            offsetTimeArray[i] = Tuple.of(segsArray[i].baseOffset, segsArray[i].lastModified());
        if (segsArray[segsArray.length - 1].size() > 0)
            offsetTimeArray[segsArray.length] = Tuple.of(log.logEndOffset(), Time.get().milliseconds());

        int startIndex;
        if (OffsetRequest.LatestTime.equals(timestamp)) {
            startIndex = offsetTimeArray.length - 1;
        } else if (OffsetRequest.EarliestTime.equals(timestamp)) {
            startIndex = 0;
        } else {
            boolean isFound = false;
            debug("Offset time array = " + Sc.map(offsetTimeArray, o -> String.format("%d, %d", o.v1, o.v2)));
            startIndex = offsetTimeArray.length - 1;
            while (startIndex >= 0 && !isFound) {
                if (offsetTimeArray[startIndex].v2 <= timestamp)
                    isFound = true;
                else
                    startIndex -= 1;
            }
        }


        int retSize = Math.min(maxNumOffsets, startIndex + 1);
        Long[] ret = new Long[retSize];
        for (int j = 0; j < retSize; j++) {
            ret[j] = offsetTimeArray[startIndex].v1;
            startIndex -= 1;
        }
        // ensure that the returned seq is in descending order of offsets;
        List<Long> list = Sc.toList(ret);
        Collections.sort(list, (a, b) -> b.intValue() - a.intValue());
        return list;
    }

    private List<TopicMetadata> getTopicMetadata(Set<String> topics) {
        List<TopicMetadata> topicResponses = metadataCache.getTopicMetadata(topics);
        if (topics.size() > 0 && topicResponses.size() != topics.size()) {
            Set<String> nonExistentTopics = Sc.subtract(topics, Sc.toSet(Sc.map(topicResponses, r -> r.topic)));
            Set<TopicMetadata> responsesForNonExistentTopics = Sc.map(nonExistentTopics, topic -> {
                if (topic == OffsetManager.OffsetsTopicName || config.autoCreateTopicsEnable) {
                    try {
                        if (topic == OffsetManager.OffsetsTopicName) {
                            List<Broker> aliveBrokers = metadataCache.getAliveBrokers();
                            int offsetsTopicReplicationFactor;
                            if (aliveBrokers.size() > 0)
                                offsetsTopicReplicationFactor = Math.min(config.offsetsTopicReplicationFactor, aliveBrokers.size());
                            else
                                offsetsTopicReplicationFactor = config.offsetsTopicReplicationFactor;
                            AdminUtils.createTopic(zkClient, topic, config.offsetsTopicPartitions,
                                    offsetsTopicReplicationFactor,
                                    offsetManager.offsetsTopicConfig());
                            info(String.format("Auto creation of topic %s with %d partitions and replication factor %d is successful!",
                                    topic, config.offsetsTopicPartitions, offsetsTopicReplicationFactor));
                        } else {
                            AdminUtils.createTopic(zkClient, topic, config.numPartitions, config.defaultReplicationFactor);
                            info(String.format("Auto creation of topic %s with %d partitions and replication factor %d is successful!",
                                    topic, config.numPartitions, config.defaultReplicationFactor));
                        }
                    } catch (TopicExistsException e) { // let it go, possibly another broker created this topic;
                    }
                    return new TopicMetadata(topic, Lists.newArrayList(), ErrorMapping.LeaderNotAvailableCode);
                } else {
                    return new TopicMetadata(topic, Lists.newArrayList(), ErrorMapping.UnknownTopicOrPartitionCode);
                }
            });
            topicResponses.addAll(responsesForNonExistentTopics);
        }
        return topicResponses;
    }

    /**
     * Service the topic metadata request API
     */
    public void handleTopicMetadataRequest(RequestChannel.Request request) {
        TopicMetadataRequest metadataRequest = (TopicMetadataRequest) request.requestObj;
        List<TopicMetadata> topicMetadata = getTopicMetadata(Sc.toSet(metadataRequest.topics));
        List<Broker> brokers = metadataCache.getAliveBrokers();
        trace(String.format("Sending topic metadata %s and brokers %s for correlation id %d to client %s", topicMetadata, brokers, metadataRequest.correlationId, metadataRequest.clientId));
        TopicMetadataResponse response = new TopicMetadataResponse(brokers, topicMetadata, metadataRequest.correlationId);
        requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)));
    }

    /*
     * Service the Offset fetch API
     */
    public void handleOffsetFetchRequest(RequestChannel.Request request) {
        OffsetFetchRequest offsetFetchRequest = (OffsetFetchRequest) request.requestObj;

        if (offsetFetchRequest.versionId == 0) {
            // version 0 reads offsets from ZK;
            List<Tuple<TopicAndPartition, OffsetMetadataAndError>> responseInfo = Sc.map(offsetFetchRequest.requestInfo, t -> {
                ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs(offsetFetchRequest.groupId, t.topic);
                try {
                    ensureTopicExists(t.topic);
                    Optional<String> payloadOpt = ZkUtils.readDataMaybeNull(zkClient, topicDirs.consumerOffsetDir() + "/" + t.partition).v1;
                    if (payloadOpt.isPresent()) {
                        return Tuple.of(t, new OffsetMetadataAndError(Long.parseLong(payloadOpt.get()), ErrorMapping.NoError));
                    }
                    return Tuple.of(t, new OffsetMetadataAndError(OffsetAndMetadata.InvalidOffset, OffsetAndMetadata.NoMetadata,
                            ErrorMapping.UnknownTopicOrPartitionCode));
                } catch (Throwable e) {
                    return Tuple.of(t, new OffsetMetadataAndError(OffsetAndMetadata.InvalidOffset, OffsetAndMetadata.NoMetadata,
                            ErrorMapping.codeFor(e.getClass())));
                }
            });
            OffsetFetchResponse response = new OffsetFetchResponse(Sc.toMap(responseInfo), offsetFetchRequest.correlationId);
            requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)));
        } else {
            // version 1 reads offsets from Kafka;
            Tuple<List<TopicAndPartition>, List<TopicAndPartition>> result = Sc.partition(offsetFetchRequest.requestInfo, topicAndPartition ->
                    metadataCache.getPartitionInfo(topicAndPartition.topic, topicAndPartition.partition).isPresent());
            List<TopicAndPartition> knownTopicPartitions = result.v1;
            List<TopicAndPartition> unknownTopicPartitions = result.v2;
            Map<TopicAndPartition, OffsetMetadataAndError> unknownStatus = Sc.toMap(Sc.map(unknownTopicPartitions, topicAndPartition ->
                    Tuple.of(topicAndPartition, OffsetMetadataAndError.UnknownTopicOrPartition)));
            Map<TopicAndPartition, OffsetMetadataAndError> knownStatus;
            if (knownTopicPartitions.size() > 0)
                knownStatus = offsetManager.getOffsets(offsetFetchRequest.groupId, knownTopicPartitions);
            else
                knownStatus = Maps.newHashMap();
            Map<TopicAndPartition, OffsetMetadataAndError> status = Sc.add(unknownStatus, knownStatus);
            OffsetFetchResponse response = new OffsetFetchResponse(status, offsetFetchRequest.correlationId);

            trace(String.format("Sending offset fetch response %s for correlation id %d to client %s.",
                    response, offsetFetchRequest.correlationId, offsetFetchRequest.clientId));
            requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)));
        }
    }

    /*
     * Service the consumer metadata API
     */
    public void handleConsumerMetadataRequest(RequestChannel.Request request) {
        ConsumerMetadataRequest consumerMetadataRequest = (ConsumerMetadataRequest) request.requestObj;

        Integer partition = offsetManager.partitionFor(consumerMetadataRequest.group);

        // get metadata (and create the topic if necessary)
        TopicMetadata offsetsTopicMetadata = Sc.head(getTopicMetadata(Sets.newHashSet(OffsetManager.OffsetsTopicName)));

        ConsumerMetadataResponse errorResponse = new ConsumerMetadataResponse(Optional.empty(), ErrorMapping.ConsumerCoordinatorNotAvailableCode, consumerMetadataRequest.correlationId);
        PartitionMetadata partitionMetadata = Sc.find(offsetsTopicMetadata.partitionsMetadata, p -> p.partitionId == partition);
        ConsumerMetadataResponse response;
        if (partitionMetadata != null) {
            if (partitionMetadata.leader != null) {
                response = new ConsumerMetadataResponse(Optional.of(partitionMetadata.leader), ErrorMapping.NoError, consumerMetadataRequest.correlationId);
            } else {
                response = errorResponse;
            }
        } else {
            response = errorResponse;
        }

        trace(String.format("Sending consumer metadata %s for correlation id %d to client %s.",
                response, consumerMetadataRequest.correlationId, consumerMetadataRequest.clientId));
        requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)));
    }

    public void close() {
        debug("Shutting down.");
        fetchRequestPurgatory.shutdown();
        producerRequestPurgatory.shutdown();
        debug("Shut down complete.");
    }

}
