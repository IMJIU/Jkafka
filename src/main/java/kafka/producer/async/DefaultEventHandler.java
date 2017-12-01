package kafka.producer.async;

import kafka.api.TopicMetadata;
import kafka.producer.Partitioner;
import kafka.utils.Logging;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.HashMap;

/**
 * @author zhoulf
 * @create 2017-12-01 58 15
 **/

public class DefaultEventHandler<K,V> extends Logging implements EventHandler<K,V> {
    ProducerConfig config;
    private Partitioner partitioner;
    private  Encoder encoder<V>;
    private  Encoder keyEncoder<K>;
    private  ProducerPool producerPool;
    private  HashMap<String, TopicMetadata> topicPartitionInfos = new HashMap<String, TopicMetadata>
        val isSync = ("sync" == config.producerType);

        val correlationId = new AtomicInteger(0);
        val brokerPartitionInfo = new BrokerPartitionInfo(config, producerPool, topicPartitionInfos);

private val topicMetadataRefreshInterval = config.topicMetadataRefreshIntervalMs;
private var lastTopicMetadataRefreshTime = 0L;
private val topicMetadataToRefresh = Set.empty<String>
private val sendPartitionPerTopicCache = HashMap.empty<String, Integer>

private val producerStats = ProducerStatsRegistry.getProducerStats(config.clientId);
private val producerTopicStats = ProducerTopicStatsRegistry.getProducerTopicStats(config.clientId);

       public void handle(Seq events<KeyedMessage<K,V>>) {
        val serializedData = serialize(events);
        serializedData.foreach {
        keyed ->
        val dataSize = keyed.message.payloadSize;
        producerTopicStats.getProducerTopicStats(keyed.topic).byteRate.mark(dataSize);
        producerTopicStats.getProducerAllTopicsStats.byteRate.mark(dataSize);
        }
        var outstandingProduceRequests = serializedData;
        var remainingRetries = config.messageSendMaxRetries + 1;
        val correlationIdStart = correlationId.get();
        debug(String.format("Handling %d events",events.size))
        while (remainingRetries > 0 && outstandingProduceRequests.size > 0) {
        topicMetadataToRefresh ++= outstandingProduceRequests.map(_.topic);
        if (topicMetadataRefreshInterval >= 0 &&;
        SystemTime.milliseconds - lastTopicMetadataRefreshTime > topicMetadataRefreshInterval) {
        Utils.swallowError(brokerPartitionInfo.updateInfo(topicMetadataToRefresh.toSet, correlationId.getAndIncrement));
        sendPartitionPerTopicCache.clear();
        topicMetadataToRefresh.clear;
        lastTopicMetadataRefreshTime = SystemTime.milliseconds;
        }
        outstandingProduceRequests = dispatchSerializedData(outstandingProduceRequests);
        if (outstandingProduceRequests.size > 0) {
        info(String.format("Back off for %d ms before retrying send. Remaining retries = %d",config.retryBackoffMs, remainingRetries-1))
        // back off and update the topic metadata cache before attempting another send operation;
        Thread.sleep(config.retryBackoffMs);
        // get topics of the outstanding produce requests and refresh metadata for those;
        Utils.swallowError(brokerPartitionInfo.updateInfo(outstandingProduceRequests.map(_.topic).toSet, correlationId.getAndIncrement));
        sendPartitionPerTopicCache.clear();
        remainingRetries -= 1;
        producerStats.resendRate.mark();
        }
        }
        if(outstandingProduceRequests.size > 0) {
        producerStats.failedSendRate.mark();
        val correlationIdEnd = correlationId.get();
        error("Failed to send requests for topics %s with correlation ids in <%d,%d>";
        .format(outstandingProduceRequests.map(_.topic).toSet.mkString(","),
        correlationIdStart, correlationIdEnd-1));
        throw new FailedToSendMessageException("Failed to send messages after " + config.messageSendMaxRetries + " tries.", null);
        }
        }

privatepublic void dispatchSerializedData(Seq messages<KeyedMessage<K,Message]>): Seq<KeyedMessage[K, Message>> = {
        val partitionedDataOpt = partitionAndCollate(messages);
        partitionedDataOpt match {
        case Some(partitionedData) ->
        val failedProduceRequests = new ArrayBuffer<KeyedMessage<K,Message>>
        try {
        for ((brokerid, messagesPerBrokerMap) <- partitionedData) {
        if (logger.isTraceEnabled)
        messagesPerBrokerMap.foreach(partitionAndEvent ->
        trace(String.format("Handling event for Topic: %s, Broker: %d, Partitions: %s",partitionAndEvent._1, brokerid, partitionAndEvent._2)))
        val messageSetPerBroker = groupMessagesToSet(messagesPerBrokerMap);

        val failedTopicPartitions = send(brokerid, messageSetPerBroker);
        failedTopicPartitions.foreach(topicPartition -> {
        messagesPerBrokerMap.get(topicPartition) match {
        case Some(data) -> failedProduceRequests.appendAll(data);
        case None -> // nothing;
        }
        });
        }
        } catch {
        case Throwable t -> error("Failed to send messages", t);
        }
        failedProduceRequests;
        case None -> // all produce requests failed;
        messages;
        }
        }

       public void serialize(Seq events<KeyedMessage<K,V]]): Seq[KeyedMessage[K,Message>> = {
        val serializedMessages = new ArrayBuffer<KeyedMessage<K,Message>>(events.size);
        events.foreach{e ->
        try {
        if(e.hasKey)
        serializedMessages += new KeyedMessage<K,Message>(topic = e.topic, key = e.key, partKey = e.partKey, message = new Message(key = keyEncoder.toBytes(e.key), bytes = encoder.toBytes(e.message)));
        else;
        serializedMessages += new KeyedMessage<K,Message>(topic = e.topic, key = e.key, partKey = e.partKey, message = new Message(bytes = encoder.toBytes(e.message)));
        } catch {
        case Throwable t ->
        producerStats.serializationErrorRate.mark();
        if (isSync) {
        throw t;
        } else {
        // currently, if in async mode, we just log the serialization error. We need to revisit;
        // this when doing kafka-496;
        error(String.format("Error serializing message for topic %s",e.topic), t)
        }
        }
        }
        serializedMessages;
        }

       public void partitionAndCollate(Seq messages<KeyedMessage<K,Message]>): Option[Map[Int, collection.mutable.Map<TopicAndPartition, Seq[KeyedMessage[K,Message]]]>> = {
        val ret = new HashMap<Integer, collection.mutable.Map<TopicAndPartition, Seq[KeyedMessage[K,Message]]>>
        try {
        for (message <- messages) {
        val topicPartitionsList = getPartitionListForTopic(message);
        val partitionIndex = getPartition(message.topic, message.partitionKey, topicPartitionsList);
        val brokerPartition = topicPartitionsList(partitionIndex);

        // postpone the failure until the send operation, so that requests for other brokers are handled correctly;
        val leaderBrokerId = brokerPartition.leaderBrokerIdOpt.getOrElse(-1);

        var HashMap dataPerBroker<TopicAndPartition, Seq<KeyedMessage[K,Message]>> = null;
        ret.get(leaderBrokerId) match {
        case Some(element) ->
        dataPerBroker = element.asInstanceOf<HashMap<TopicAndPartition, Seq[KeyedMessage[K,Message]]>>
        case None ->
        dataPerBroker = new HashMap<TopicAndPartition, Seq<KeyedMessage[K,Message]>>
        ret.put(leaderBrokerId, dataPerBroker);
        }

        val topicAndPartition = TopicAndPartition(message.topic, brokerPartition.partitionId);
        var ArrayBuffer dataPerTopicPartition<KeyedMessage<K,Message>> = null;
        dataPerBroker.get(topicAndPartition) match {
        case Some(element) ->
        dataPerTopicPartition = element.asInstanceOf<ArrayBuffer<KeyedMessage[K,Message]>>
        case None ->
        dataPerTopicPartition = new ArrayBuffer<KeyedMessage<K,Message>>
        dataPerBroker.put(topicAndPartition, dataPerTopicPartition);
        }
        dataPerTopicPartition.append(message);
        }
        Some(ret);
        }catch {    // Swallow recoverable exceptions and return None so that they can be retried.;
        case UnknownTopicOrPartitionException ute -> warn("Failed to collate messages by topic,partition due to: " + ute.getMessage); None;
        case LeaderNotAvailableException lnae -> warn("Failed to collate messages by topic,partition due to: " + lnae.getMessage); None;
        case Throwable oe -> error("Failed to collate messages by topic, partition due to: " + oe.getMessage); None;
        }
        }

privatepublic void getPartitionListForTopic(KeyedMessage m<K,Message]): Seq[PartitionAndLeader> = {
        val topicPartitionsList = brokerPartitionInfo.getBrokerPartitionInfo(m.topic, correlationId.getAndIncrement);
        debug("Broker partitions registered for topic: %s are %s";
        .format(m.topic, topicPartitionsList.map(p -> p.partitionId).mkString(",")))
        val totalNumPartitions = topicPartitionsList.length;
        if(totalNumPartitions == 0)
        throw new NoBrokersForPartitionException("Partition key = " + m.key);
        topicPartitionsList;
        }

/**
 * Retrieves the partition id and throws an UnknownTopicOrPartitionException if
 * the value of partition is not between 0 and numPartitions-1
 * @param topic The topic
 * @param key the partition key
 * @param topicPartitionList the list of available partitions
 * @return the partition id
 */
privatepublic Integer  void getPartition(String topic, Object key, Seq topicPartitionList<PartitionAndLeader>) {
        val numPartitions = topicPartitionList.size;
        if(numPartitions <= 0)
        throw new UnknownTopicOrPartitionException("Topic " + topic + " doesn't exist");
        val partition =
        if(key == null) {
        // If the key is null, we don't really need a partitioner;
        // So we look up in the send partition cache for the topic to decide the target partition;
        val id = sendPartitionPerTopicCache.get(topic);
        id match {
        case Some(partitionId) ->
        // directly return the partitionId without checking availability of the leader,
        // since we want to postpone the failure until the send operation anyways;
        partitionId;
        case None ->
        val availablePartitions = topicPartitionList.filter(_.leaderBrokerIdOpt.isDefined);
        if (availablePartitions.isEmpty)
        throw new LeaderNotAvailableException("No leader for any partition in topic " + topic)
        val index = Utils.abs(Random.nextInt) % availablePartitions.size;
        val partitionId = availablePartitions(index).partitionId;
        sendPartitionPerTopicCache.put(topic, partitionId);
        partitionId;
        }
        } else;
        partitioner.partition(key, numPartitions);
        if(partition < 0 || partition >= numPartitions)
        throw new UnknownTopicOrPartitionException("Invalid partition id: " + partition + " for topic " + topic +
        "; Valid values are in the inclusive range of [0, " + (numPartitions-1) + "]");
        trace(String.format("Assigning message of topic %s and key %s to a selected partition %d",topic, if (key == null) "<none>" else key.toString, partition))
        partition;
        }

/**
 * Constructs and sends the produce request based on a map from (topic, partition) -> messages
 *
 * @param brokerId the broker that will receive the request
 * @param messagesPerTopic the messages as a map from (topic, partition) -> messages
 * @return the set (topic, partitions) messages which incurred an error sending or processing
 */
privatepublic void send(Int brokerId, collection messagesPerTopic.mutable.Map<TopicAndPartition, ByteBufferMessageSet>) = {
        if(brokerId < 0) {
        warn(String.format("Failed to send data since partitions %s don't have a leader",messagesPerTopic.map(_._1).mkString(",")))
        messagesPerTopic.keys.toSeq;
        } else if(messagesPerTopic.size > 0) {
        val currentCorrelationId = correlationId.getAndIncrement;
        val producerRequest = new ProducerRequest(currentCorrelationId, config.clientId, config.requestRequiredAcks,
        config.requestTimeoutMs, messagesPerTopic);
        var failedTopicPartitions = Seq.empty<TopicAndPartition>
        try {
        val syncProducer = producerPool.getProducer(brokerId);
        debug("Producer sending messages with correlation id %d for topics %s to broker %d on %s:%d";
        .format(currentCorrelationId, messagesPerTopic.keySet.mkString(","), brokerId, syncProducer.config.host, syncProducer.config.port))
        val response = syncProducer.send(producerRequest);
        debug("Producer sent messages with correlation id %d for topics %s to broker %d on %s:%d";
        .format(currentCorrelationId, messagesPerTopic.keySet.mkString(","), brokerId, syncProducer.config.host, syncProducer.config.port))
        if(response != null) {
        if (response.status.size != producerRequest.data.size)
        throw new KafkaException(String.format("Incomplete response (%s) for producer request (%s)",response, producerRequest))
        if (logger.isTraceEnabled) {
        val successfullySentData = response.status.filter(_._2.error == ErrorMapping.NoError);
        successfullySentData.foreach(m -> messagesPerTopic(m._1).foreach(message ->
        trace(String.format("Successfully sent message: %s",if(message.message.isNull) null else Utils.readString(message.message.payload)))))
        }
        val failedPartitionsAndStatus = response.status.filter(_._2.error != ErrorMapping.NoError).toSeq;
        failedTopicPartitions = failedPartitionsAndStatus.map(partitionStatus -> partitionStatus._1);
        if(failedTopicPartitions.size > 0) {
        val errorString = failedPartitionsAndStatus;
        .sortWith((p1, p2) -> p1._1.topic.compareTo(p2._1.topic) < 0 ||;
        (p1._1.topic.compareTo(p2._1.topic) == 0 && p1._1.partition < p2._1.partition));
        .map{
        case(topicAndPartition, status) ->
        topicAndPartition.toString + ": " + ErrorMapping.exceptionFor(status.error).getClass.getName;
        }.mkString(",");
        warn(String.format("Produce request with correlation id %d failed due to %s",currentCorrelationId, errorString))
        }
        failedTopicPartitions;
        } else {
        Seq.empty<TopicAndPartition>
        }
        } catch {
        case Throwable t ->
        warn("Failed to send producer request with correlation id %d to broker %d with data for partitions %s";
        .format(currentCorrelationId, brokerId, messagesPerTopic.map(_._1).mkString(",")), t)
        messagesPerTopic.keys.toSeq;
        }
        } else {
        List.empty;
        }
        }

privatepublic void groupMessagesToSet(collection messagesPerTopicAndPartition.mutable.Map<TopicAndPartition, Seq<KeyedMessage[K,Message]>>) = {
        /** enforce the compressed.topics config here.
         *  If the compression codec is anything other than NoCompressionCodec,
         *    Enable compression only for specified topics if any
         *    If the list of compressed topics is empty, then enable the specified compression codec for all topics
         *  If the compression codec is NoCompressionCodec, compression is disabled for all topics
         */

        val messagesPerTopicPartition = messagesPerTopicAndPartition.map { case (topicAndPartition, messages) ->
        val rawMessages = messages.map(_.message);
        ( topicAndPartition,
        config.compressionCodec match {
        case NoCompressionCodec ->
        debug(String.format("Sending %d messages with no compression to %s",messages.size, topicAndPartition))
        new ByteBufferMessageSet(NoCompressionCodec, _ rawMessages*);
        case _ ->
        config.compressedTopics.size match {
        case 0 ->
        debug("Sending %d messages with compression codec %d to %s";
        .format(messages.size, config.compressionCodec.codec, topicAndPartition))
        new ByteBufferMessageSet(config.compressionCodec, _ rawMessages*);
        case _ ->
        if(config.compressedTopics.contains(topicAndPartition.topic)) {
        debug("Sending %d messages with compression codec %d to %s";
        .format(messages.size, config.compressionCodec.codec, topicAndPartition))
        new ByteBufferMessageSet(config.compressionCodec, _ rawMessages*);
        }
        else {
        debug("Sending %d messages to %s with no compression as it is not in compressed.topics - %s";
        .format(messages.size, topicAndPartition, config.compressedTopics.toString))
        new ByteBufferMessageSet(NoCompressionCodec, _ rawMessages*);
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
