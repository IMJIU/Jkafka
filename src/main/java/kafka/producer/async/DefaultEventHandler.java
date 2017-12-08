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
    private HashMap<String, TopicMetadata> topicPartitionInfos = new HashMap<String, TopicMetadata>();
    public AtomicInteger correlationId = new AtomicInteger(0);
    public BrokerPartitionInfo brokerPartitionInfo = new BrokerPartitionInfo(config, producerPool, topicPartitionInfos);

    private Integer topicMetadataRefreshInterval = config.topicMetadataRefreshIntervalMs;
    private Long lastTopicMetadataRefreshTime = 0L;
    private Set<String> topicMetadataToRefresh = Sets.newHashSet();
    private HashMap<String, Integer> sendPartitionPerTopicCache = Maps.newHashMap();

    private ProducerStats producerStats = ProducerStatsRegistry.getProducerStats(config.clientId());
    private ProducerTopicStats producerTopicStats = ProducerTopicStatsRegistry.getProducerTopicStats(config.clientId());
    boolean isSync;

    public DefaultEventHandler(ProducerConfig config, Partitioner partitioner, Encoder<V> encoder, Encoder<K> keyEncoder, ProducerPool producerPool, HashMap<String, TopicMetadata> topicPartitionInfos) {
        this.config = config;
        this.partitioner = partitioner;
        this.encoder = encoder;
        this.keyEncoder = keyEncoder;
        this.producerPool = producerPool;
        this.topicPartitionInfos = topicPartitionInfos;
        isSync = ("sync" == config.producerType);
    }


    public void handle(List<KeyedMessage<K, V>> events) {
        List<KeyedMessage<K, Message>> serializedData = serialize(events);
        serializedData.forEach(
                keyed -> {
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
            if (topicMetadataRefreshInterval >= 0 &&
                    Time.get().milliseconds() - lastTopicMetadataRefreshTime > topicMetadataRefreshInterval) {
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
                // get topics of the outstanding produce requests and refresh metadata for those;
                Utils.swallowError(() -> brokerPartitionInfo.updateInfo(Sc.toSet(Sc.map(outstandingProduceRequests, r -> r.topic)), correlationId.getAndIncrement()));
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
        if(partitionedDataOpt.isPresent()){

        }
        partitionedDataOpt match {
            case Some(partitionedData)->
                val failedProduceRequests = new ArrayBuffer<KeyedMessage<K, Message>>
                try {
                    for ((brokerid, messagesPerBrokerMap)<-partitionedData){
                        if (logger.isTraceEnabled)
                            messagesPerBrokerMap.foreach(partitionAndEvent ->
                                    trace(String.format("Handling event for Topic: %s, Broker: %d, Partitions: %s", partitionAndEvent._1, brokerid, partitionAndEvent._2)))
                        val messageSetPerBroker = groupMessagesToSet(messagesPerBrokerMap);

                        val failedTopicPartitions = send(brokerid, messageSetPerBroker);
                        failedTopicPartitions.foreach(topicPartition -> {
                            messagesPerBrokerMap.get(topicPartition) match {
                                case Some(data)->failedProduceRequests.appendAll(data);
                                case None -> // nothing;
                            }
                        });
                    }
                } catch {
                case Throwable t -> error("Failed to send messages", t);
            }
            failedProduceRequests;
            case None -> // all produce requests failed;
                    messages ;
        }
    }

    public List<KeyedMessage<K, Message>> serialize(List<KeyedMessage<K, V>> events) {
        final List<KeyedMessage<K, Message>> serializedMessages = new ArrayList<KeyedMessage<K, Message>>(events.size());
        events.forEach(e -> {
            try {
                if (e.hasKey())
                    serializedMessages.add(new KeyedMessage<K, Message>(e.topic, e.key, e.partKey, new Message(keyEncoder.toBytes(e.key), encoder.toBytes(e.message))));
                else
                    serializedMessages.add(new KeyedMessage<K, Message>(e.topic, e.key, e.partKey, new Message(encoder.toBytes(e.message))));
            } catch (Throwable t) {
                producerStats.serializationErrorRate.mark();
                if (isSync) {
                    throw t;
                } else {
                    // currently, if in async mode, we just log the serialization error. We need to revisit;
                    // this when doing kafka-496;
                    error(String.format("Error serializing message for topic %s", e.topic), t)
                }
            }
        });
        return serializedMessages;
    }

    public Optional<Map<Integer, Map<TopicAndPartition, List<KeyedMessage<K, Message>>>>> partitionAndCollate(List<KeyedMessage<K, Message>> messages)

    {
        HashMap<Integer, Map<TopicAndPartition, List<KeyedMessage<K, Message>>>> ret = new HashMap<>();
        try {
            for (KeyedMessage<K, Message> message : messages) {
                val topicPartitionsList = getPartitionListForTopic(message);
                val partitionIndex = getPartition(message.topic, message.partitionKey, topicPartitionsList);
                val brokerPartition = topicPartitionsList(partitionIndex);

                // postpone the failure until the send operation, so that requests for other brokers are handled correctly;
                val leaderBrokerId = brokerPartition.leaderBrokerIdOpt.getOrElse(-1);

                var HashMap dataPerBroker<TopicAndPartition, Seq<KeyedMessage[K, Message]>>=null;
                ret.get(leaderBrokerId) match {
                    case Some(element)->
                        dataPerBroker = element.asInstanceOf < HashMap < TopicAndPartition, Seq[KeyedMessage[K, Message]]>>
                    case None ->
                            dataPerBroker = new HashMap<TopicAndPartition, Seq<KeyedMessage[K,Message]>>
                        ret.put(leaderBrokerId, dataPerBroker);
                }

                val topicAndPartition = TopicAndPartition(message.topic, brokerPartition.partitionId);
                var ArrayBuffer dataPerTopicPartition<KeyedMessage<K, Message>>=null;
                dataPerBroker.get(topicAndPartition) match {
                    case Some(element)->
                        dataPerTopicPartition = element.asInstanceOf < ArrayBuffer < KeyedMessage[K, Message]>>
                    case None ->
                            dataPerTopicPartition = new ArrayBuffer<KeyedMessage<K, Message>>
                        dataPerBroker.put(topicAndPartition, dataPerTopicPartition);
                }
                dataPerTopicPartition.append(message);
            }
            Some(ret);
        } catch {    // Swallow recoverable exceptions and return None so that they can be retried.;
        case UnknownTopicOrPartitionException ute -> warn("Failed to collate messages by topic,partition due to: " + ute.getMessage);
            None;
        case LeaderNotAvailableException lnae -> warn("Failed to collate messages by topic,partition due to: " + lnae.getMessage);
            None;
        case Throwable oe -> error("Failed to collate messages by topic, partition due to: " + oe.getMessage);
            None;
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
            warn(String.format("Failed to send data since partitions %s don't have a leader", messagesPerTopic.map(_._1).mkString(",")))
            return Sc.toList(messagesPerTopic.keySet());
        } else if (messagesPerTopic.size() > 0) {
            int currentCorrelationId = correlationId.getAndIncrement();
            ProducerRequest producerRequest = new ProducerRequest(currentCorrelationId, config.clientId(), config.requestRequiredAcks(),
                    config.requestTimeoutMs(), messagesPerTopic);
            List<TopicAndPartition> failedTopicPartitions = Lists.newArrayList();
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
                    Map<TopicAndPartition, ProducerResponseStatus> failedPartitionsAndStatus = Sc.filter(response.status, (k, v) -> v.error != ErrorMapping.NoError);
                    failedTopicPartitions = Sc.map(failedPartitionsAndStatus, (k, v) -> k);
                    if (failedTopicPartitions.size() > 0) {
                        Map<TopicAndPartition, ProducerResponseStatus> errorString = failedPartitionsAndStatus;
                        .sortWith((p1, p2) -> p1._1.topic.compareTo(p2._1.topic) < 0 ||;
                        (p1._1.topic.compareTo(p2._1.topic) == 0 && p1._1.partition < p2._1.partition));
                        .map {
                            case (topicAndPartition, status) ->
                                    topicAndPartition.toString + ": " + ErrorMapping.exceptionFor(status.error).getClass.getName ;
                        }.mkString(",");
                        warn(String.format("Produce request with correlation id %d failed due to %s", currentCorrelationId, errorString))
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

    private void groupMessagesToSet(collection messagesPerTopicAndPartition.mutable.Map<TopicAndPartition, Seq<KeyedMessage[K, Message]>>)

    =

    {
        /** enforce the compressed.topics config here.
         *  If the compression codec is anything other than NoCompressionCodec,
         *    Enable compression only for specified topics if any
         *    If the list of compressed topics is empty, then enable the specified compression codec for all topics
         *  If the compression codec is NoCompressionCodec, compression is disabled for all topics
         */

        val messagesPerTopicPartition = messagesPerTopicAndPartition.map {
        case (topicAndPartition, messages) ->
                val rawMessages = messages.map(_.message);
            (topicAndPartition,
                    config.compressionCodec match {
            case NoCompressionCodec ->
                    debug(String.format("Sending %d messages with no compression to %s", messages.size, topicAndPartition))
                new ByteBufferMessageSet(NoCompressionCodec, _ rawMessages *);
            case _ ->
                    config.compressedTopics.size match {
                case 0->
                    debug("Sending %d messages with compression codec %d to %s";
                    .format(messages.size, config.compressionCodec.codec, topicAndPartition))
                    new ByteBufferMessageSet(config.compressionCodec, _ rawMessages *);
                case _ ->
                    if (config.compressedTopics.contains(topicAndPartition.topic)) {
                        debug("Sending %d messages with compression codec %d to %s";
                        .format(messages.size, config.compressionCodec.codec, topicAndPartition))
                        new ByteBufferMessageSet(config.compressionCodec, _ rawMessages *);
                    } else {
                        debug("Sending %d messages to %s with no compression as it is not in compressed.topics - %s";
                        .format(messages.size, topicAndPartition, config.compressedTopics.toString))
                        new ByteBufferMessageSet(NoCompressionCodec, _ rawMessages *);
                    }
            }
        }
        );
    }
        messagesPerTopicPartition;
    }

    public void close() {
        if (producerPool != null)
            producerPool.close;
    }
}
