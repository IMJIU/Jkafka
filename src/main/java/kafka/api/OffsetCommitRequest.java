package kafka.api;/**
 * Created by zhoulf on 2017/5/15.
 */

import kafka.common.ErrorMapping;
import kafka.common.OffsetAndMetadata;
import kafka.func.IntCount;
import kafka.func.Tuple;
import kafka.log.TopicAndPartition;
import kafka.network.BoundedByteBufferSend;
import kafka.network.RequestChannel;
import kafka.utils.Prediction;
import kafka.utils.Time;
import kafka.utils.Utils;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author
 * @create 2017-05-15 00 18
 **/
public class OffsetCommitRequest extends RequestOrResponse {
    public static Short CurrentVersion = 1;
    public static String DefaultClientId = "";
    public String groupId;
    public Map<TopicAndPartition, OffsetAndMetadata> requestInfo;
    public Short versionId;
    public Integer correlationId;
    public String clientId;
    public Integer groupGenerationId = org.apache.kafka.common.requests.OffsetCommitRequest.DEFAULT_GENERATION_ID;
    public String consumerId = org.apache.kafka.common.requests.OffsetCommitRequest.DEFAULT_CONSUMER_ID;
    //lazy
    public Map<String, Map<TopicAndPartition, OffsetAndMetadata>> requestInfoGroupedByTopic;

    public OffsetCommitRequest(java.lang.String groupId, Map<TopicAndPartition, OffsetAndMetadata> requestInfo, Short versionId, Integer correlationId, java.lang.String clientId, Integer groupGenerationId, java.lang.String consumerId) {
        this.groupId = groupId;
        this.requestInfo = requestInfo;
        this.versionId = versionId;
        this.correlationId = correlationId;
        this.clientId = clientId;
        this.groupGenerationId = groupGenerationId;
        this.consumerId = consumerId;
        requestInfoGroupedByTopic = Utils.groupByKey(requestInfo, t -> t.topic);
    }

    public static OffsetCommitRequest readFrom(ByteBuffer buffer) {
        // Read values from the envelope;
        Short versionId = buffer.getShort();
        Prediction.Assert(versionId == 0 || versionId == 1,
                "Version " + versionId + " is invalid for OffsetCommitRequest. Valid versions are 0 or 1.");

        Integer correlationId = buffer.getInt();
        String clientId = ApiUtils.readShortString(buffer);

        // Read the OffsetRequest;
        String consumerGroupId = ApiUtils.readShortString(buffer);

        // version 1 specific fields;
        Integer groupGenerationId = org.apache.kafka.common.requests.OffsetCommitRequest.DEFAULT_GENERATION_ID;
        String consumerId = org.apache.kafka.common.requests.OffsetCommitRequest.DEFAULT_CONSUMER_ID;
        if (versionId == 1) {
            groupGenerationId = buffer.getInt();
            consumerId = ApiUtils.readShortString(buffer);
        }

        Integer topicCount = buffer.getInt();
        List<Tuple<TopicAndPartition, OffsetAndMetadata>> pairs = Stream.iterate(1, n -> n + 1).limit(topicCount).flatMap(n -> {
            String topic = ApiUtils.readShortString(buffer);
            Integer partitionCount = buffer.getInt();
            return Stream.iterate(1, m -> m + 1).limit(partitionCount).map(m -> {
                Integer partitionId = buffer.getInt();
                Long offset = buffer.getLong();
                Long timestamp;
                if (versionId == 1) {
                    Long given = buffer.getLong();
                    timestamp = given;
                } else {
                    timestamp = OffsetAndMetadata.InvalidTime;
                }
                String metadata = ApiUtils.readShortString(buffer);
                return Tuple.of(new TopicAndPartition(topic, partitionId), new OffsetAndMetadata(offset, metadata, timestamp));
            });
        }).collect(Collectors.toList());
        return new OffsetCommitRequest(consumerGroupId, Utils.toMap(pairs), versionId, correlationId, clientId, groupGenerationId, consumerId);
    }

    public static void changeInvalidTimeToCurrentTime(OffsetCommitRequest offsetCommitRequest) {
        Long now = Time.get().milliseconds();
        for (Map.Entry<TopicAndPartition, OffsetAndMetadata> entry : offsetCommitRequest.requestInfo.entrySet()) {
            if (entry.getValue().timestamp == OffsetAndMetadata.InvalidTime)
                entry.getValue().timestamp = now;
        }
    }

    public Stream<Map.Entry<TopicAndPartition, OffsetAndMetadata>> filterLargeMetadata(Integer maxMetadataSize) {
//           requestInfo.filter(info => info._2.metadata == null || info._2.metadata.length <= maxMetadataSize);
        return requestInfo.entrySet().stream().filter(info -> info.getValue().metadata == null || info.getValue().metadata.length() <= maxMetadataSize);
    }

    public OffsetCommitResponse responseFor(Short errorCode, Integer offsetMetadataMaxSize) {
        Map<TopicAndPartition, Short> commitStatus = Utils.mapValue(requestInfo, v -> {
            if (v.metadata != null && v.metadata.length() > offsetMetadataMaxSize)
                return ErrorMapping.OffsetMetadataTooLargeCode;
            else if (errorCode == ErrorMapping.UnknownTopicOrPartitionCode)
                return ErrorMapping.ConsumerCoordinatorNotAvailableCode;
            else if (errorCode == ErrorMapping.NotLeaderForPartitionCode)
                return ErrorMapping.NotCoordinatorForConsumerCode;
            else
                return errorCode;
        });
        return new OffsetCommitResponse(commitStatus, correlationId);
    }

    public void writeTo(ByteBuffer buffer) {
        // Write envelope;
        buffer.putShort(versionId);
        buffer.putInt(correlationId);
        ApiUtils.writeShortString(buffer, clientId);

        // Write OffsetCommitRequest;
        ApiUtils.writeShortString(buffer, groupId);             // consumer group;

        // version 1 specific data;
        if (versionId == 1) {
            buffer.putInt(groupGenerationId);
            ApiUtils.writeShortString(buffer, consumerId);
        }
        buffer.putInt(requestInfoGroupedByTopic.size());// number of topics;
        requestInfoGroupedByTopic.forEach((t1, v) -> { // topic -> Map<TopicAndPartition, OffsetMetadataAndError>
            ApiUtils.writeShortString(buffer, t1); // topic;
            buffer.putInt(v.size());       // number of partitions for this topic;
            v.forEach((t2, metadata) -> {
                buffer.putInt(t2.partition);
                buffer.putLong(metadata.offset);
                if (versionId == 1)
                    buffer.putLong(metadata.timestamp);
                ApiUtils.writeShortString(buffer, metadata.metadata);
            });
        });
    }

    @Override
    public Integer sizeInBytes() {
        final IntCount count = IntCount.of(0);
        requestInfoGroupedByTopic.forEach((topic, offsets) -> {
            final AtomicInteger innerCount = new AtomicInteger(0);
            offsets.forEach((k, v) -> innerCount.getAndAdd(
                    4 /* partition */ +
                            8 /* offset */ +
                            ((versionId == 1) ? 8 : 0) /* timestamp */
                            +
                            ApiUtils.shortStringLength(v.metadata)));
            count.add(ApiUtils.shortStringLength(topic) + /* topic */
                    4  /* number of partitions */ + innerCount.get());
        });
        return 2 + /* versionId */
                4 + /* correlationId */
                ApiUtils.shortStringLength(clientId) +
                ApiUtils.shortStringLength(groupId) +
                ((versionId == 1) ? 4 /* group generation id */ +
                        ApiUtils.shortStringLength(consumerId) : 0) +
                4 + /* topic count */
                count.get();
    }


    @Override
    public void handleError(Throwable e, RequestChannel requestChannel, RequestChannel.Request request) {
        Short errorCode = ErrorMapping.codeFor(e.getClass());
        OffsetCommitResponse errorResponse = responseFor(errorCode, Integer.MAX_VALUE);
        requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(errorResponse)));
    }

    @Override
    public String describe(Boolean details) {
        StringBuilder offsetCommitRequest = new StringBuilder();
        offsetCommitRequest.append("Name: " + this.getClass().getSimpleName());
        offsetCommitRequest.append("; Version: " + versionId);
        offsetCommitRequest.append("; CorrelationId: " + correlationId);
        offsetCommitRequest.append("; ClientId: " + clientId);
        offsetCommitRequest.append("; GroupId: " + groupId);
        offsetCommitRequest.append("; GroupGenerationId: " + groupGenerationId);
        offsetCommitRequest.append("; ConsumerId: " + consumerId);
        if (details)
            offsetCommitRequest.append("; RequestInfo: " + requestInfo);
        return offsetCommitRequest.toString();
    }

    @Override
    public String toString() {
        return describe(true);
    }
}
