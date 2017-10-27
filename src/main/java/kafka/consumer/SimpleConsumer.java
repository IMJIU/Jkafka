package kafka.consumer;

/**
 * @author zhoulf
 * @create 2017-10-24 30 18
 **/

import kafka.api.*;
import kafka.network.BlockingChannel;
import kafka.network.Receive;
import kafka.utils.Logging;

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
    }

    public void init() {
        ConsumerConfig.validateClientId(clientId);
    }

    private Object lock = new Object();
    private BlockingChannel blockingChannel = new BlockingChannel(host, port, bufferSize, BlockingChannel.UseDefaultBufferSize, soTimeout);
    private FetchRequestAndResponseStats fetchRequestAndResponseStats = FetchRequestAndResponseStatsRegistry.getFetchRequestAndResponseStats(clientId);
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

    private Receive sendRequest(RequestOrResponse request) throws ClosedChannelException {
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
           return  response;
        }
    }

    public TopicMetadataResponse send(TopicMetadataRequest request) throws ClosedChannelException {
        Receive response = sendRequest(request);
        TopicMetadataResponse.readFrom(response.buffer());
    }

    public ConsumerMetadataResponse send(ConsumerMetadataRequest request) {
        val response = sendRequest(request);
        ConsumerMetadataResponse.readFrom(response.buffer);
    }

    /**
     * Fetch a set of messages from a topic.
     *
     * @param request specifies the topic name, topic partition, starting byte offset, maximum bytes to be fetched.
     * @return a set of fetched messages
     */
    public FetchResponse fetch(FetchRequest request) {
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
     * Get a list of valid offsets (up to maxSize) before the given time.
     *
     * @param request a <<kafka.api.OffsetRequest>> object.
     * @return a <<kafka.api.OffsetResponse>> object.
     */
    public void getOffsetsBefore(OffsetRequest request) =OffsetResponse.readFrom(

    sendRequest(request).buffer)

    /**
     * Commit offsets for a topic
     * Version 0 of the request will commit offsets to Zookeeper and version 1 and above will commit offsets to Kafka.
     *
     * @param request a <<kafka.api.OffsetCommitRequest>> object.
     * @return a <<kafka.api.OffsetCommitResponse>> object.
     */
    public void commitOffsets(OffsetCommitRequest request) =

    {
        // With TODO KAFKA-1012, we have to first issue a ConsumerMetadataRequest and connect to the coordinator before;
        // we can commit offsets.;
        OffsetCommitResponse.readFrom(sendRequest(request).buffer);
    }

    /**
     * Fetch offsets for a topic
     * Version 0 of the request will fetch offsets from Zookeeper and version 1 version 1 and above will fetch offsets from Kafka.
     *
     * @param request a <<kafka.api.OffsetFetchRequest>> object.
     * @return a <<kafka.api.OffsetFetchResponse>> object.
     */
    public void fetchOffsets(OffsetFetchRequest request) =OffsetFetchResponse.readFrom(

    sendRequest(request).buffer);

    private void getOrMakeConnection() {
        if (!isClosed && !blockingChannel.isConnected) {
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
    public Long

    void earliestOrLatestOffset(TopicAndPartition topicAndPartition, Long earliestOrLatest, Int consumerId) {
        val request = OffsetRequest(requestInfo = Map(topicAndPartition -> PartitionOffsetRequestInfo(earliestOrLatest, 1)),
                clientId = clientId,
                replicaId = consumerId);
        val partitionErrorAndOffset = getOffsetsBefore(request).partitionErrorAndOffsets(topicAndPartition)
        val offset = partitionErrorAndOffset.error match {
            case ErrorMapping.NoError =>partitionErrorAndOffset.offsets.head;
            case _ =>throw ErrorMapping.exceptionFor(partitionErrorAndOffset.error);
        }
        offset;
    }
}
