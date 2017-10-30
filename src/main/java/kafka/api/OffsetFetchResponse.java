package kafka.api;

import kafka.common.OffsetMetadataAndError;
import kafka.func.IntCount;
import kafka.func.Tuple;
import kafka.log.TopicAndPartition;
import kafka.utils.Sc;

import java.nio.ByteBuffer;
import java.util.*;

import static kafka.api.ApiUtils.*;

/**
 * @author zhoulf
 * @create 2017-10-30 25 15
 **/

public class OffsetFetchResponse extends RequestOrResponse {
    public static final Short CurrentVersion = 0;
    public Map<TopicAndPartition, OffsetMetadataAndError> requestInfo;
    public Integer correlationId = 0;
    // TODO: 2017/10/30 lazy
    private Map<String, Map<TopicAndPartition, OffsetMetadataAndError>> requestInfoGroupedByTopic = Sc.groupByKey(requestInfo, r -> r.topic);

    public Map<String, Map<TopicAndPartition, OffsetMetadataAndError>> requestInfoGroupedByTopic() {
        if (requestInfoGroupedByTopic == null) {
            requestInfoGroupedByTopic = Sc.groupByKey(requestInfo, r -> r.topic);
        }
        return requestInfoGroupedByTopic;
    }


    public OffsetFetchResponse(Map<TopicAndPartition, OffsetMetadataAndError> requestInfo, Integer correlationId) {
        this.requestInfo = requestInfo;
        this.correlationId = correlationId;
    }

    public static OffsetFetchResponse readFrom(ByteBuffer buffer) {
        int correlationId = buffer.getInt();
        int topicCount = buffer.getInt();
        List<Tuple<TopicAndPartition, OffsetMetadataAndError>> pairs = Sc.itFlatToList(1, topicCount, n -> {
            String topic = readShortString(buffer);
            int partitionCount = buffer.getInt();
            return Sc.itToList(1, partitionCount, m -> {
                int partitionId = buffer.getInt();
                long offset = buffer.getLong();
                String metadata = readShortString(buffer);
                short error = buffer.getShort();
                return Tuple.of(new TopicAndPartition(topic, partitionId), new OffsetMetadataAndError(offset, metadata, error));
            }).stream();
        });
        return new OffsetFetchResponse(Sc.toMap(pairs), correlationId);
    }


    public void writeTo(ByteBuffer buffer) {
        buffer.putInt(correlationId);
        buffer.putInt(requestInfoGroupedByTopic.size()); // number of topics;
        requestInfoGroupedByTopic.forEach((topic, map) -> { // topic -> Map<TopicAndPartition, OffsetMetadataAndError>
            writeShortString(buffer, topic); // topic;
            buffer.putInt(map.size());       // number of partitions for this topic;
            map.forEach((topicAndPartition, offsetMetadataAndError) -> { // TopicAndPartition -> OffsetMetadataAndError;
                buffer.putInt(topicAndPartition.partition);
                buffer.putLong(offsetMetadataAndError.offset);
                writeShortString(buffer, offsetMetadataAndError.metadata);
                buffer.putShort(offsetMetadataAndError.error);
            });
        });
    }

    @Override
    public Integer sizeInBytes() {
        IntCount size = IntCount.of(
                4 + /* correlationId */
                        4 /* topic count */);
        requestInfoGroupedByTopic.forEach((topic, offsets) -> {
            size.add(shortStringLength(topic) + /* topic */ 4 /* number of partitions */);
            offsets.forEach((t, offsetsAndMetadata) ->
                    size.add(4 /* partition */ +
                            8 /* offset */ +
                            shortStringLength(offsetsAndMetadata.metadata) +
                            2 /* error */)
            );
        });
        return size.get();
    }


    @Override
    public String describe(Boolean details) {
        return this.toString();
    }
}
