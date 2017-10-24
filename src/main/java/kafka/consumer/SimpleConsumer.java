package kafka.consumer;

/**
 * @author zhoulf
 * @create 2017-10-24 30 18
 **/

/**
 * A consumer of kafka messages
 */
//@threadsafe
class SimpleConsumer(val String host,
        val Int port,
        val Int soTimeout,
        val Int bufferSize,
        val String clientId) extends Logging {

        ConsumerConfig.validateClientId(clientId);
private val lock = new Object();
private val blockingChannel = new BlockingChannel(host, port, bufferSize, BlockingChannel.UseDefaultBufferSize, soTimeout);
private val fetchRequestAndResponseStats = FetchRequestAndResponseStatsRegistry.getFetchRequestAndResponseStats(clientId);
private var isClosed = false;

privatepublic BlockingChannel  void connect() {
        close;
        blockingChannel.connect();
        blockingChannel;
        }

privatepublic void disconnect() = {
        debug("Disconnecting from " + formatAddress(host, port))
        blockingChannel.disconnect();
        }

privatepublic void reconnect() {
        disconnect();
        connect();
        }

       public void close() {
        lock synchronized {
        disconnect();
        isClosed = true;
        }
        }

privatepublic Receive  void sendRequest(RequestOrResponse request) {
        lock synchronized {
        var Receive response = null;
        try {
        getOrMakeConnection();
        blockingChannel.send(request);
        response = blockingChannel.receive();
        } catch {
        case e : Throwable =>
        info(String.format("Reconnect due to socket error: %s",e.toString))
        // retry once;
        try {
        reconnect();
        blockingChannel.send(request);
        response = blockingChannel.receive();
        } catch {
        case Throwable e =>
        disconnect();
        throw e;
        }
        }
        response;
        }
        }

       public TopicMetadataResponse  void send(TopicMetadataRequest request) {
        val response = sendRequest(request);
        TopicMetadataResponse.readFrom(response.buffer);
        }

       public ConsumerMetadataResponse  void send(ConsumerMetadataRequest request) {
        val response = sendRequest(request);
        ConsumerMetadataResponse.readFrom(response.buffer);
        }

        /**
         *  Fetch a set of messages from a topic.
         *
         *  @param request  specifies the topic name, topic partition, starting byte offset, maximum bytes to be fetched.
         *  @return a set of fetched messages
         */
       public FetchResponse  void fetch(FetchRequest request) {
        var Receive response = null;
        val specificTimer = fetchRequestAndResponseStats.getFetchRequestAndResponseStats(host, port).requestTimer;
        val aggregateTimer = fetchRequestAndResponseStats.getFetchRequestAndResponseAllBrokersStats.requestTimer;
        aggregateTimer.time {
        specificTimer.time {
        response = sendRequest(request);
        }
        }
        val fetchResponse = FetchResponse.readFrom(response.buffer);
        val fetchedSize = fetchResponse.sizeInBytes;
        fetchRequestAndResponseStats.getFetchRequestAndResponseStats(host, port).requestSizeHist.update(fetchedSize);
        fetchRequestAndResponseStats.getFetchRequestAndResponseAllBrokersStats.requestSizeHist.update(fetchedSize);
        fetchResponse;
        }

        /**
         *  Get a list of valid offsets (up to maxSize) before the given time.
         *  @param request a <<kafka.api.OffsetRequest>> object.
         *  @return a <<kafka.api.OffsetResponse>> object.
         */
       public void getOffsetsBefore(OffsetRequest request) = OffsetResponse.readFrom(sendRequest(request).buffer)

        /**
         * Commit offsets for a topic
         * Version 0 of the request will commit offsets to Zookeeper and version 1 and above will commit offsets to Kafka.
         * @param request a <<kafka.api.OffsetCommitRequest>> object.
         * @return a <<kafka.api.OffsetCommitResponse>> object.
         */
       public void commitOffsets(OffsetCommitRequest request) = {
        // With TODO KAFKA-1012, we have to first issue a ConsumerMetadataRequest and connect to the coordinator before;
        // we can commit offsets.;
        OffsetCommitResponse.readFrom(sendRequest(request).buffer);
        }

        /**
         * Fetch offsets for a topic
         * Version 0 of the request will fetch offsets from Zookeeper and version 1 version 1 and above will fetch offsets from Kafka.
         * @param request a <<kafka.api.OffsetFetchRequest>> object.
         * @return a <<kafka.api.OffsetFetchResponse>> object.
         */
       public void fetchOffsets(OffsetFetchRequest request) = OffsetFetchResponse.readFrom(sendRequest(request).buffer);

privatepublic void getOrMakeConnection() {
        if(!isClosed && !blockingChannel.isConnected) {
        connect();
        }
        }

        /**
         * Get the earliest or latest offset of a given topic, partition.
         * @param topicAndPartition Topic and partition of which the offset is needed.
         * @param earliestOrLatest A value to indicate earliest or latest offset.
         * @param consumerId Id of the consumer which could be a consumer client, SimpleConsumerShell or a follower broker.
         * @return Requested offset.
         */
       public Long  void earliestOrLatestOffset(TopicAndPartition topicAndPartition, Long earliestOrLatest, Int consumerId) {
        val request = OffsetRequest(requestInfo = Map(topicAndPartition -> PartitionOffsetRequestInfo(earliestOrLatest, 1)),
        clientId = clientId,
        replicaId = consumerId);
        val partitionErrorAndOffset = getOffsetsBefore(request).partitionErrorAndOffsets(topicAndPartition)
        val offset = partitionErrorAndOffset.error match {
        case ErrorMapping.NoError => partitionErrorAndOffset.offsets.head;
        case _ => throw ErrorMapping.exceptionFor(partitionErrorAndOffset.error);
        }
        offset;
        }
        }
