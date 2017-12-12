package kafka.consumer;

/**
 * @author zhoulf
 * @create 2017-10-24 30 18
 **/

import com.google.common.collect.ImmutableMap;
import kafka.api.*;
import kafka.common.ErrorMapping;
import kafka.log.TopicAndPartition;
import kafka.metrics.KafkaTimer;
import kafka.network.BlockingChannel;
import kafka.network.Receive;
import kafka.utils.Logging;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;

import static org.apache.kafka.common.utils.Utils.*;

/**
 * A consumer of kafka messages
 */
//@threadsafe
public class SimpleConsumer extends Logging {
    public String host;
    public Integer port;
    public Integer soTimeout;
    public Integer bufferSize;
    public String clientId;

    public SimpleConsumer(String host, Integer port, Integer soTimeout, Integer bufferSize, String clientId) {
        this.host = host;
        this.port = port;
        this.soTimeout = soTimeout;
        this.bufferSize = bufferSize;
        this.clientId = clientId;
        blockingChannel = new BlockingChannel(host, port, bufferSize, BlockingChannel.UseDefaultBufferSize, soTimeout);
        fetchRequestAndResponseStats = FetchRequestAndResponseStatsRegistry.getFetchRequestAndResponseStats(clientId);

    }

    public void init() {
        ConsumerConfig.validateClientId(clientId);
    }

    private Object lock = new Object();
    private BlockingChannel blockingChannel ;
    private FetchRequestAndResponseStats fetchRequestAndResponseStats ;
    private boolean isClosed = false;

    private BlockingChannel connect() {
        close();
        blockingChannel.connect();
        return blockingChannel;
    }

    private void disconnect() {
        debug("Disconnecting from " + formatAddress(host, port));
        blockingChannel.disconnect();
    }

    private void reconnect() {
        disconnect();
        connect();
    }

    public void close() {
        synchronized (lock) {
            disconnect();
            isClosed = true;
        }
    }

    private Receive sendRequest(RequestOrResponse request) throws IOException {
        synchronized (lock) {
            Receive response = null;
            try {
                getOrMakeConnection();
                blockingChannel.send(request);
                response = blockingChannel.receive();
            } catch (Throwable e) {
                info(String.format("Reconnect due to socket error: %s", e.toString()));
                // retry once;
                try {
                    reconnect();
                    blockingChannel.send(request);
                    response = blockingChannel.receive();
                } catch (Throwable e2) {
                    disconnect();
                    throw e2;
                }
            }
            return response;
        }
    }

    public TopicMetadataResponse send(TopicMetadataRequest request) throws IOException {
        Receive response = sendRequest(request);
        return TopicMetadataResponse.readFrom(response.buffer());
    }

    public ConsumerMetadataResponse send(ConsumerMetadataRequest request) throws IOException {
        Receive response = null;
        response = sendRequest(request);
        return ConsumerMetadataResponse.readFrom(response.buffer());
    }

    /**
     * Fetch a set of messages from a topic.
     *
     * @param request specifies the topic name, topic partition, starting byte offset, maximum bytes to be fetched.
     * @return a set of fetched messages
     */
    public FetchResponse fetch(FetchRequest request) {
        Receive response;
        KafkaTimer specificTimer = fetchRequestAndResponseStats.getFetchRequestAndResponseStats(host, port).requestTimer;
        KafkaTimer aggregateTimer = fetchRequestAndResponseStats.getFetchRequestAndResponseAllBrokersStats().requestTimer;
        response = aggregateTimer.time(() ->
                specificTimer.time(() -> {
                    try {
                        return sendRequest(request);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
        );
        FetchResponse fetchResponse = FetchResponse.readFrom(response.buffer());
        Integer fetchedSize = fetchResponse.sizeInBytes();
        fetchRequestAndResponseStats.getFetchRequestAndResponseStats(host, port).requestSizeHist.update(fetchedSize);
        fetchRequestAndResponseStats.getFetchRequestAndResponseAllBrokersStats().requestSizeHist.update(fetchedSize);
        return fetchResponse;
    }

    /**
     * Get a list of valid offsets (up to maxSize) before the given time.
     *
     * @param request a <<kafka.api.OffsetRequest>> object.
     * @return a <<kafka.api.OffsetResponse>> object.
     */
    public OffsetResponse getOffsetsBefore(OffsetRequest request) throws IOException {
        return OffsetResponse.readFrom(sendRequest(request).buffer());
    }

    /**
     * Commit offsets for a topic
     * Version 0 of the request will commit offsets to Zookeeper and version 1 and above will commit offsets to Kafka.
     *
     * @param request a <<kafka.api.OffsetCommitRequest>> object.
     * @return a <<kafka.api.OffsetCommitResponse>> object.
     */
    public OffsetCommitResponse commitOffsets(OffsetCommitRequest request) throws IOException {
        // With TODO KAFKA-1012, we have to first issue a ConsumerMetadataRequest and connect to the coordinator before;
        // we can commit offsets.;
        return OffsetCommitResponse.readFrom(sendRequest(request).buffer());
    }

    /**
     * Fetch offsets for a topic
     * Version 0 of the request will fetch offsets from Zookeeper and version 1 version 1 and above will fetch offsets from Kafka.
     *
     * @param request a <<kafka.api.OffsetFetchRequest>> object.
     * @return a <<kafka.api.OffsetFetchResponse>> object.
     */
    public OffsetFetchResponse fetchOffsets(OffsetFetchRequest request) throws IOException {
        return OffsetFetchResponse.readFrom(sendRequest(request).buffer());
    }

    private void getOrMakeConnection() {
        if (!isClosed && !blockingChannel.isConnected()) {
            connect();
        }
    }

    /**
     * Get the earliest or latest offset of a given topic, partition.
     *
     * @param topicAndPartition Topic and partition of which the offset is needed.
     * @param earliestOrLatest  A value to indicate earliest or latest offset.
     * @param consumerId        Id of the consumer which could be a consumer client, SimpleConsumerShell or a follower broker.
     * @return Requested offset.
     */
    public Long earliestOrLatestOffset(TopicAndPartition topicAndPartition, Long earliestOrLatest, Integer consumerId) throws Throwable{
        OffsetRequest request = new OffsetRequest(ImmutableMap.of(topicAndPartition, new PartitionOffsetRequestInfo(earliestOrLatest, 1)),
                clientId, consumerId);
        PartitionOffsetsResponse partitionErrorAndOffset = getOffsetsBefore(request).partitionErrorAndOffsets.get(topicAndPartition);
        if(ErrorMapping.NoError == partitionErrorAndOffset.error){
            return partitionErrorAndOffset.offsets.get(0);
        }
        throw ErrorMapping.exceptionFor(partitionErrorAndOffset.error);
    }
}
