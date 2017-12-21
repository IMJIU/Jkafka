package kafka.api;

import kafka.common.ErrorMapping;
import kafka.func.Handler;
import kafka.func.IntCount;
import kafka.func.Tuple;
import kafka.log.TopicAndPartition;
import kafka.message.ByteBufferMessageSet;
import kafka.network.BoundedByteBufferSend;
import kafka.network.RequestChannel;
import kafka.utils.Utils;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by Administrator on 2017/4/22.
 */
public class ProducerRequest extends RequestOrResponse {
    public final static Short CurrentVersion = 0;

    public Short versionId = ProducerRequest.CurrentVersion;
    public Integer correlationId;
    public String clientId;
    public Short requiredAcks;
    public Integer ackTimeoutMs;
    public Map<TopicAndPartition, ByteBufferMessageSet> data;
    /**
     * Partitions the data into a map of maps (one for each topic).
     */
    private Map<String, Map<TopicAndPartition, ByteBufferMessageSet>> dataGroupedByTopic;
    public Map<TopicAndPartition, Integer> topicPartitionMessageSizeMap;

    public ProducerRequest(Integer correlationId,
                           String clientId,
                           Short requiredAcks,
                           Integer ackTimeoutMs,
                           Map<TopicAndPartition, ByteBufferMessageSet> data) {
        this(ProducerRequest.CurrentVersion, correlationId, clientId, requiredAcks, ackTimeoutMs, data);
    }


    public ProducerRequest(Short versionId, Integer correlationId, String clientId,
                           Short requiredAcks,
                           Integer ackTimeoutMs,
                           Map<TopicAndPartition, ByteBufferMessageSet> data) {
        super(Optional.of(RequestKeys.ProduceKey));
        this.versionId = versionId;
        this.correlationId = correlationId;
        this.clientId = clientId;
        this.requiredAcks = requiredAcks;
        this.ackTimeoutMs = ackTimeoutMs;
        this.data = data;
        dataGroupedByTopic = Utils.groupByKey(data, k -> k.topic);
        topicPartitionMessageSizeMap = Utils.mapValue(data, r -> r.sizeInBytes());
    }


    public final static Handler<ByteBuffer, ProducerRequest> readFrom = (buffer) -> {
        Short versionId = buffer.getShort();
        Integer correlationId = buffer.getInt();
        String clientId = ApiUtils.readShortString(buffer);
        Short requiredAcks = buffer.getShort();
        Integer ackTimeoutMs = buffer.getInt();
        //build the topic structure;
        Integer topicCount = buffer.getInt();
        List<Tuple<TopicAndPartition, ByteBufferMessageSet>> partitionDataPairs =
                Stream.iterate(1, n -> n + 1).limit(topicCount).flatMap(n -> {
                    // process topic;
                    String topic = ApiUtils.readShortString(buffer);
                    Integer partitionCount = buffer.getInt();
                    List<Tuple<TopicAndPartition, ByteBufferMessageSet>> list =
                            Stream.iterate(1, m -> m + 1).limit(partitionCount).map(p -> {
                                Integer partition = buffer.getInt();
                                Integer messageSetSize = buffer.getInt();
                                byte[] messageSetBuffer = new byte[messageSetSize];
                                buffer.get(messageSetBuffer, 0, messageSetSize);
                                return Tuple.of(new TopicAndPartition(topic, partition),
                                        new ByteBufferMessageSet(ByteBuffer.wrap(messageSetBuffer)));
                            }).collect(Collectors.toList());
                    return list.stream();
                }).collect(Collectors.toList());
        return new ProducerRequest(versionId, correlationId, clientId, requiredAcks, ackTimeoutMs, Utils.toMap(partitionDataPairs));
    };


    @Override
    public void writeTo(ByteBuffer buffer) {
        buffer.putShort(versionId);
        buffer.putInt(correlationId);
        ApiUtils.writeShortString(buffer, clientId);
        buffer.putShort(requiredAcks);
        buffer.putInt(ackTimeoutMs);

        //save the topic structure;
        buffer.putInt(dataGroupedByTopic.size()); //the number of topics;
        for (Map.Entry<String, Map<TopicAndPartition, ByteBufferMessageSet>> entry : dataGroupedByTopic.entrySet()) {
            String topic = entry.getKey();
            Map<TopicAndPartition, ByteBufferMessageSet> topicAndPartitionData = entry.getValue();
            ApiUtils.writeShortString(buffer, topic); //write the topic;
            buffer.putInt(topicAndPartitionData.size()); //the number of partitions;
            topicAndPartitionData.entrySet().forEach(partitionAndData -> {
                Integer partition = partitionAndData.getKey().partition;
                ByteBufferMessageSet partitionMessageData = partitionAndData.getValue();
                ByteBuffer bytes = partitionMessageData.buffer;
                buffer.putInt(partition);
                buffer.putInt(bytes.limit());
                buffer.put(bytes);
                bytes.rewind();
            });
        }
    }

    public Integer sizeInBytes() {
        IntCount foldedTopics = IntCount.of(0);
        dataGroupedByTopic.entrySet().stream().forEach(currTopic -> {
            AtomicInteger foldedPartitions = new AtomicInteger(0);
            currTopic.getValue().entrySet().forEach(currPartition ->
                    foldedPartitions.set(foldedPartitions.intValue() +
                            4 + /* partition id */
                            4 + /* byte-length of serialized messages */
                            currPartition.getValue().sizeInBytes())
            );
            foldedTopics.set(foldedTopics.get() +
                    ApiUtils.shortStringLength(currTopic.getKey()) +
                    4 + /* the number of partitions */
                    foldedPartitions.intValue()
            );
        });
        return 2 + /* versionId */
                4 + /* correlationId */
                ApiUtils.shortStringLength(clientId) + /* client id */
                2 + /* requiredAcks */
                4 + /* ackTimeoutMs */
                4 + /* number of topics */
                foldedTopics.get();
    }


    public Integer numPartitions() {
        return data.size();
    }

    @Override
    public String toString() {
        return describe(true);
    }


    public void handleError(Exception e, RequestChannel requestChannel, RequestChannel.Request request) {
        if (((ProducerRequest) request.requestObj).requiredAcks == 0) {
            requestChannel.closeConnection(request.processor, request);
        } else {
            Map<TopicAndPartition, ProducerResponseStatus> producerResponseStatus =
                    Utils.mapValue(data, v -> new ProducerResponseStatus(ErrorMapping.codeFor(e.getClass()), -1l));
            ProducerResponse errorResponse = new ProducerResponse(correlationId, producerResponseStatus);
            requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(errorResponse)));
        }
    }

    //
    @Override
    public String describe(Boolean details) {
        StringBuilder producerRequest = new StringBuilder();
        producerRequest.append("Name: " + this.getClass().getSimpleName());
        producerRequest.append("; Version: " + versionId);
        producerRequest.append("; CorrelationId: " + correlationId);
        producerRequest.append("; ClientId: " + clientId);
        producerRequest.append("; RequiredAcks: " + requiredAcks);
        producerRequest.append("; AckTimeoutMs: " + ackTimeoutMs + " ms");
        if (details)
            producerRequest.append("; TopicAndPartition: " + topicPartitionMessageSizeMap);
        return producerRequest.toString();
    }

    public void emptyData() {
        data.clear();
    }
}


