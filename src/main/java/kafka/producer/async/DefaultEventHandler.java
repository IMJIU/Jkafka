package kafka.producer.async;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import kafka.api.ProducerRequest;
import kafka.api.ProducerResponse;
import kafka.api.ProducerResponseStatus;
import kafka.api.TopicMetadata;
import kafka.common.ErrorMapping;
import kafka.common.FailedToSendMessageException;
import kafka.common.KafkaException;
import kafka.common.NoBrokersForPartitionException;
import kafka.func.Tuple;
import kafka.log.TopicAndPartition;
import kafka.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.producer.*;
import kafka.serializer.Encoder;
import kafka.utils.Logging;
import kafka.utils.Sc;
import kafka.utils.Time;
import kafka.utils.Utils;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static kafka.message.CompressionCodec.*;

/**
 * @author zhoulf
 * @create 2017-12-01 58 15
 **/

public class DefaultEventHandler<K, V> extends Logging implements EventHandler<K, V> {
    public ProducerConfig config;
    private Partitioner partitioner;
    private Encoder<V> encoder;
    private Encoder<K> keyEncoder;
    private ProducerPool producerPool;
    private HashMap<String, TopicMetadata> topicPartitionInfos;
    public AtomicInteger correlationId = new AtomicInteger(0);
    public BrokerPartitionInfo brokerPartitionInfo;

    private Integer topicMetadataRefreshInterval;
    private Long lastTopicMetadataRefreshTime = 0L;
    private Set<String> topicMetadataToRefresh = Sets.newHashSet();
    private HashMap<String, Integer> sendPartitionPerTopicCache = Maps.newHashMap();

    private ProducerStats producerStats;
    private ProducerTopicStats producerTopicStats;
    boolean isSync;

    public DefaultEventHandler(ProducerConfig config, Partitioner partitioner, Encoder<V> encoder, Encoder<K> keyEncoder, ProducerPool producerPool, HashMap<String, TopicMetadata> topicPartitionInfos) {
        this.config = config;
        this.partitioner = partitioner;
        this.encoder = encoder;
        this.keyEncoder = keyEncoder;
        this.producerPool = producerPool;
        this.topicPartitionInfos = topicPartitionInfos == null ? new HashMap<>() : topicPartitionInfos;
        isSync = ("sync" == config.producerType);
        brokerPartitionInfo = new BrokerPartitionInfo(config, producerPool, this.topicPartitionInfos);
        topicMetadataRefreshInterval = config.topicMetadataRefreshIntervalMs;
        producerStats = ProducerStatsRegistry.getProducerStats(config.clientId());
        producerTopicStats = ProducerTopicStatsRegistry.getProducerTopicStats(config.clientId());
    }


    public void handle(List<KeyedMessage<K, V>> events) {
        List<KeyedMessage<K, Message>> serializedData = serialize(events);
        serializedData.forEach(keyed -> {
            Integer dataSize = keyed.message.payloadSize();
            producerTopicStats.getProducerTopicStats(keyed.topic).byteRate.mark(dataSize);
            producerTopicStats.getProducerAllTopicsStats().byteRate.mark(dataSize);
        });
        List<KeyedMessage<K, Message>> outstandingProduceRequests = serializedData;
        int remainingRetries = config.messageSendMaxRetries + 1;
        Integer correlationIdStart = correlationId.get();
        debug(String.format("Handling %d events", events.size()));
        while (remainingRetries > 0 && outstandingProduceRequests.size() > 0) {
            topicMetadataToRefresh.addAll(Sc.map(outstandingProduceRequests, r -> r.topic));
            if (topicMetadataRefreshInterval >= 0
                    && Time.get().milliseconds() - lastTopicMetadataRefreshTime > topicMetadataRefreshInterval) {
                Utils.swallowError(() -> brokerPartitionInfo.updateInfo(topicMetadataToRefresh, correlationId.getAndIncrement()));
                sendPartitionPerTopicCache.clear();
                topicMetadataToRefresh.clear();
                lastTopicMetadataRefreshTime = Time.get().milliseconds();
            }
            outstandingProduceRequests = dispatchSerializedData(outstandingProduceRequests);
            if (outstandingProduceRequests.size() > 0) {
                info(String.format("Back off for %d ms before retrying send. Remaining retries = %d", config.retryBackoffMs, remainingRetries - 1));
                // back off and update the topic metadata cache before attempting another send operation;
                try {
                    Thread.sleep(config.retryBackoffMs);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                List<KeyedMessage<K, Message>> outstandingProduceRequests2 = outstandingProduceRequests;
                // get topics of the outstanding produce requests and refresh metadata for those;
                Utils.swallowError(() -> brokerPartitionInfo.updateInfo(Sc.toSet(Sc.map(outstandingProduceRequests2, r -> r.topic)), correlationId.getAndIncrement()));
                sendPartitionPerTopicCache.clear();
                remainingRetries -= 1;
                producerStats.resendRate.mark();
            }
        }
        if (outstandingProduceRequests.size() > 0) {
            producerStats.failedSendRate.mark();
            Integer correlationIdEnd = correlationId.get();
            error(String.format("Failed to send requests for topics %s with correlation ids in <%d,%d>",
                    Sc.mkString(Sc.toSet(Sc.map(outstandingProduceRequests, r -> r.topic)), ","),
                    correlationIdStart, correlationIdEnd - 1));
            throw new FailedToSendMessageException("Failed to send messages after " + config.messageSendMaxRetries + " tries.", null);
        }
    }

    private List<KeyedMessage<K, Message>> dispatchSerializedData(List<KeyedMessage<K, Message>> messages) {
        Optional<Map<Integer, Map<TopicAndPartition, List<KeyedMessage<K, Message>>>>> partitionedDataOpt = partitionAndCollate(messages);
        if (partitionedDataOpt.isPresent()) {
            Map<Integer, Map<TopicAndPartition, List<KeyedMessage<K, Message>>>> partitionedData = partitionedDataOpt.get();
            List<KeyedMessage<K, Message>> failedProduceRequests = Lists.newArrayList();
            try {
                for (Map.Entry<Integer, Map<TopicAndPartition, List<KeyedMessage<K, Message>>>> entry : partitionedData.entrySet()) {
                    Integer brokerId = entry.getKey();
                    Map<TopicAndPartition, List<KeyedMessage<K, Message>>> messagesPerBrokerMap = entry.getValue();
                    if (logger.isTraceEnabled())
                        messagesPerBrokerMap.forEach((k, v) -> trace(String.format("Handling event for Topic: %s, Broker: %d, Partitions: %s", k, brokerId, v)));
                    Map<TopicAndPartition, ByteBufferMessageSet> messageSetPerBroker = groupMessagesToSet(messagesPerBrokerMap);

                    List<TopicAndPartition> failedTopicPartitions = send(brokerId, messageSetPerBroker);
                    failedTopicPartitions.forEach(topicPartition -> {
                        List<KeyedMessage<K, Message>> data = messagesPerBrokerMap.get(topicPartition);
                        if (data != null) {
                            failedProduceRequests.addAll(data);
                        }
                    });
                }
            } catch (Throwable t) {
                error("Failed to send messages", t);
            }
            return failedProduceRequests;
        } else { // all produce requests failed;
            return messages;
        }
    }

    public List<KeyedMessage<K, Message>> serialize(List<KeyedMessage<K, V>> events) {
        final List<KeyedMessage<K, Message>> serializedMessages = new ArrayList<>(events.size());
        events.forEach(e -> {
            try {
                if (e.hasKey())
                    serializedMessages.add(new KeyedMessage<>(e.topic, e.key, e.partKey, new Message(keyEncoder.toBytes(e.key), encoder.toBytes(e.message))));
                else
                    serializedMessages.add(new KeyedMessage<>(e.topic, e.key, e.partKey, new Message(encoder.toBytes(e.message))));
            } catch (Throwable t) {
                producerStats.serializationErrorRate.mark();
                if (isSync) {
                    throw t;
                } else {
                    // currently, if in async mode, we just log the serialization error. We need to revisit;
                    // this when doing kafka-496;
                    error(String.format("Error serializing message for topic %s", e.topic), t);
                }
            }
        });
        return serializedMessages;
    }

    public Optional<Map<Integer, Map<TopicAndPartition, List<KeyedMessage<K, Message>>>>> partitionAndCollate(List<KeyedMessage<K, Message>> messages) {
        HashMap<Integer, Map<TopicAndPartition, List<KeyedMessage<K, Message>>>> ret = new HashMap<>();
        try {
            for (KeyedMessage<K, Message> message : messages) {
                List<PartitionAndLeader> topicPartitionsList = getPartitionListForTopic(message);
                Integer partitionIndex = getPartition(message.topic, message.partitionKey(), topicPartitionsList);
                PartitionAndLeader brokerPartition = topicPartitionsList.get(partitionIndex);

                // postpone the failure until the send operation, so that requests for other brokers are handled correctly;
                Integer leaderBrokerId = brokerPartition.leaderBrokerIdOpt.orElse(-1);

                HashMap<TopicAndPartition, List<KeyedMessage<K, Message>>> dataPerBroker;
                Map<TopicAndPartition, List<KeyedMessage<K, Message>>> element = ret.get(leaderBrokerId);
                if (element != null) {
                    dataPerBroker = (HashMap<TopicAndPartition, List<KeyedMessage<K, Message>>>) element;
                } else {
                    dataPerBroker = Maps.newHashMap();
                    ret.put(leaderBrokerId, dataPerBroker);
                }

                TopicAndPartition topicAndPartition = new TopicAndPartition(message.topic, brokerPartition.partitionId);
                List<KeyedMessage<K, Message>> dataPerTopicPartition;
                List<KeyedMessage<K, Message>> element2 = dataPerBroker.get(topicAndPartition);
                if (element2 != null) {
                    dataPerTopicPartition = element2;
                } else {
                    dataPerTopicPartition = Lists.newArrayList();
                    dataPerBroker.put(topicAndPartition, dataPerTopicPartition);
                }
                dataPerTopicPartition.add(message);
            }
            return Optional.of(ret);
        } catch (UnknownTopicOrPartitionException ute) {    // Swallow recoverable exceptions and return None so that they can be retried.;
            warn("Failed to collate messages by topic,partition due to: " + ute.getMessage());
            return Optional.empty();
        } catch (LeaderNotAvailableException lnae) {
            warn("Failed to collate messages by topic,partition due to: " + lnae.getMessage());
            return Optional.empty();
        } catch (Throwable oe) {
            error("Failed to collate messages by topic, partition due to: " + oe.getMessage());
            return Optional.empty();
        }
    }

    private List<PartitionAndLeader> getPartitionListForTopic(KeyedMessage<K, Message> m) {
        List<PartitionAndLeader> topicPartitionsList = brokerPartitionInfo.getBrokerPartitionInfo(m.topic, correlationId.getAndIncrement());
        debug(String.format("Broker partitions registered for topic: %s are %s",
                m.topic, Sc.mkString(Sc.map(topicPartitionsList, p -> p.partitionId), ",")));
        int totalNumPartitions = topicPartitionsList.size();
        if (totalNumPartitions == 0)
            throw new NoBrokersForPartitionException("Partition key = " + m.key);
        return topicPartitionsList;
    }

    /**
     * Retrieves the partition id and throws an UnknownTopicOrPartitionException if
     * the value of partition is not between 0 and numPartitions-1
     *
     * @param topic              The topic
     * @param key                the partition key
     * @param topicPartitionList the list of available partitions
     * @return the partition id
     */
    private Integer getPartition(String topic, Object key, List<PartitionAndLeader> topicPartitionList) {
        int numPartitions = topicPartitionList.size();
        if (numPartitions <= 0)
            throw new UnknownTopicOrPartitionException("Topic " + topic + " doesn't exist");
        Integer partition;
        if (key == null) {
            // If the key is null, we don't really need a partitioner;
            // So we look up in the send partition cache for the topic to decide the target partition;
            Integer id = sendPartitionPerTopicCache.get(topic);
            if (id != null) {
                // directly return the partitionId without checking availability of the leader,
                // since we want to postpone the failure until the send operation anyways;
                partition = id;
            } else {
                List<PartitionAndLeader> availablePartitions = Sc.filter(topicPartitionList, t -> t.leaderBrokerIdOpt.isPresent());
                if (availablePartitions.isEmpty())
                    throw new LeaderNotAvailableException("No leader for any partition in topic " + topic);
                int index = Utils.abs(new Random().nextInt()) % availablePartitions.size();
                Integer partitionId = availablePartitions.get(index).partitionId;
                sendPartitionPerTopicCache.put(topic, partitionId);
                partition = partitionId;
            }
        } else {
            partition = partitioner.partition(key, numPartitions);
        }
        if (partition < 0 || partition >= numPartitions)
            throw new UnknownTopicOrPartitionException("Invalid partition id: " + partition + " for topic " + topic +
                    "; Valid values are in the inclusive range of [0, " + (numPartitions - 1) + "]");
        trace(String.format("Assigning message of topic %s and key %s to a selected partition %d", topic,
                (key == null) ? "<none>" : key.toString(), partition));
        return partition;
    }

    /**
     * Constructs and sends the produce request based on a map from (topic, partition) -> messages
     *
     * @param brokerId         the broker that will receive the request
     * @param messagesPerTopic the messages as a map from (topic, partition) -> messages
     * @return the set (topic, partitions) messages which incurred an error sending or processing
     */
    private List<TopicAndPartition> send(Integer brokerId, Map<TopicAndPartition, ByteBufferMessageSet> messagesPerTopic) {
        if (brokerId < 0) {
            warn(String.format("Failed to send data since partitions %s don't have a leader", Sc.mkString(Sc.map(messagesPerTopic, (k, v) -> k), ",")));
            return Sc.toList(messagesPerTopic.keySet());
        } else if (messagesPerTopic.size() > 0) {
            int currentCorrelationId = correlationId.getAndIncrement();
            ProducerRequest producerRequest = new ProducerRequest(currentCorrelationId, config.clientId(), config.requestRequiredAcks(),
                    config.requestTimeoutMs(), messagesPerTopic);
            List<TopicAndPartition> failedTopicPartitions;
            try {
                SyncProducer syncProducer = producerPool.getProducer(brokerId);
                debug(String.format("Producer sending messages with correlation id %d for topics %s to broker %d on %s:%d",
                        currentCorrelationId, Sc.mkString(messagesPerTopic.keySet(), ","), brokerId, syncProducer.config.host, syncProducer.config.port));
                ProducerResponse response = syncProducer.send(producerRequest);
                debug(String.format("Producer sent messages with correlation id %d for topics %s to broker %d on %s:%d"
                        , currentCorrelationId, Sc.mkString(messagesPerTopic.keySet(), ","), brokerId, syncProducer.config.host, syncProducer.config.port));
                if (response != null) {
                    if (response.status.size() != producerRequest.data.size())
                        throw new KafkaException(String.format("Incomplete response (%s) for producer request (%s)", response, producerRequest));
                    if (logger.isTraceEnabled()) {
                        Map<TopicAndPartition, ProducerResponseStatus> successfullySentData = Sc.filter(response.status, (k, v) -> v.error == ErrorMapping.NoError);
                        successfullySentData.forEach((k, v) -> messagesPerTopic.get(k).forEach(message ->
                                trace(String.format("Successfully sent message: %s",
                                        (message.message.isNull()) ? null : Utils.readString(message.message.payload())))));
                    }
                    List<Tuple<TopicAndPartition, ProducerResponseStatus>> failedPartitionsAndStatus = Sc.toList(Sc.filter(response.status, (k, v) -> v.error != ErrorMapping.NoError));
                    failedTopicPartitions = Sc.map(failedPartitionsAndStatus, s -> s.v1);
                    if (failedTopicPartitions.size() > 0) {
                        List<Tuple<TopicAndPartition, ProducerResponseStatus>> list = Sc.sortWith(failedPartitionsAndStatus, (p1, p2) -> p1.v1.topic.compareTo(p2.v1.topic) < 0
                                || (p1.v1.topic.compareTo(p2.v1.topic) == 0
                                && p1.v1.partition < p2.v1.partition));
                        String errorString = Sc.mkString(Sc.map(list, (topicAndPartition, status) ->
                                topicAndPartition.toString() + ": " + ErrorMapping.exceptionFor(status.error).getClass().getName()), ",");

                        warn(String.format("Produce request with correlation id %d failed due to %s", currentCorrelationId, errorString));
                    }
                    return failedTopicPartitions;
                } else {
                    return Collections.EMPTY_LIST;
                }
            } catch (Throwable t) {
                warn(String.format("Failed to send producer request with correlation id %d to broker %d with data for partitions %s",
                        currentCorrelationId, brokerId, Sc.mkString(Sc.map(messagesPerTopic, (k, v) -> k), ",")), t);
                return Sc.toList(messagesPerTopic.keySet());
            }
        } else {
            return Collections.EMPTY_LIST;
        }
    }

    private Map<TopicAndPartition, ByteBufferMessageSet> groupMessagesToSet(Map<TopicAndPartition, List<KeyedMessage<K, Message>>> messagesPerTopicAndPartition) {
        /** enforce the compressed.topics config here.
         *  If the compression codec is anything other than NoCompressionCodec,
         *    Enable compression only for specified topics if any
         *    If the list of compressed topics is empty, then enable the specified compression codec for all topics
         *  If the compression codec is NoCompressionCodec, compression is disabled for all topics
         */
        List<Tuple<TopicAndPartition, ByteBufferMessageSet>> list = Sc.map(messagesPerTopicAndPartition, (topicAndPartition, messages) -> {
            List<Message> rawMessages = Sc.map(messages, m -> m.message);
            ByteBufferMessageSet byteBufferMessageSet = null;
            switch (config.compressionCodec) {
                case NoCompressionCodec:
                    debug(String.format("Sending %d messages with no compression to %s", messages.size(), topicAndPartition));
                    byteBufferMessageSet = new ByteBufferMessageSet(NoCompressionCodec, rawMessages);
                    break;
                case GZIPCompressionCodec:
                case LZ4CompressionCodec:
                case SnappyCompressionCodec:
                    if (config.compressedTopics.size() == 0) {
                        debug(String.format("Sending %d messages with compression codec %d to %s",
                                messages.size(), config.compressionCodec.codec, topicAndPartition));
                        byteBufferMessageSet = new ByteBufferMessageSet(config.compressionCodec, rawMessages);
                    } else {
                        if (config.compressedTopics.contains(topicAndPartition.topic)) {
                            debug(String.format("Sending %d messages with compression codec %d to %s",
                                    messages.size(), config.compressionCodec.codec, topicAndPartition));
                            byteBufferMessageSet = new ByteBufferMessageSet(config.compressionCodec, rawMessages);
                        } else {
                            debug(String.format("Sending %d messages to %s with no compression as it is not in compressed.topics - %s",
                                    messages.size(), topicAndPartition, config.compressedTopics.toString()));
                            byteBufferMessageSet = new ByteBufferMessageSet(NoCompressionCodec, rawMessages);
                        }
                    }
            }
            return Tuple.of(topicAndPartition, byteBufferMessageSet);
        });
        Map<TopicAndPartition, ByteBufferMessageSet> messagesPerTopicPartition = Sc.toMap(list);
        return messagesPerTopicPartition;
    }

    public void close() {
        if (producerPool != null)
            producerPool.close();
    }
}
