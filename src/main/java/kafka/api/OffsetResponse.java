package kafka.api;

import kafka.common.ErrorMapping;
import kafka.func.IntCount;
import kafka.func.Tuple;
import kafka.log.TopicAndPartition;
import kafka.utils.Sc;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static kafka.api.ApiUtils.*;

/**
 * @author zhoulf
 * @create 2017-10-30 46 10
 **/


public class OffsetResponse extends RequestOrResponse {
    public Integer correlationId;
    public Map<TopicAndPartition, PartitionOffsetsResponse> partitionErrorAndOffsets;
    private Map<String, Map<TopicAndPartition, PartitionOffsetsResponse>> offsetsGroupedByTopic;

    public OffsetResponse(Integer correlationId, Map<TopicAndPartition, PartitionOffsetsResponse> partitionErrorAndOffsets) {
        this.correlationId = correlationId;
        this.partitionErrorAndOffsets = partitionErrorAndOffsets;
    }

    // TODO: 2017/10/30 lazy
    public Map<String, Map<TopicAndPartition, PartitionOffsetsResponse>> offsetsGroupedByTopic() {
        if (offsetsGroupedByTopic == null) {
            offsetsGroupedByTopic = Sc.groupByKey(partitionErrorAndOffsets, t -> t.topic);
        }
        return offsetsGroupedByTopic;
    }

    public static OffsetResponse readFrom(ByteBuffer buffer) {
        int correlationId = buffer.getInt();
        int numTopics = buffer.getInt();
        List<Tuple<TopicAndPartition, PartitionOffsetsResponse>> pairs = Sc.itFlatToList(1, numTopics, n -> {
            String topic = readShortString(buffer);
            int numPartitions = buffer.getInt();
            return Sc.itToList(1, numPartitions, m -> {
                int partition = buffer.getInt();
                short error = buffer.getShort();
                int numOffsets = buffer.getInt();
                List<Long> offsets = Sc.itToList(1, numOffsets, l -> buffer.getLong());
                return Tuple.of(new TopicAndPartition(topic, partition), new PartitionOffsetsResponse(error, offsets));
            }).stream();
        });
        return new OffsetResponse(correlationId, Sc.toMap(pairs));
    }


    public boolean hasError() {
        return Sc.exists(partitionErrorAndOffsets.values(), p -> p.error != ErrorMapping.NoError);
    }

    public Integer sizeInBytes() {
        IntCount intCount = IntCount.of(4 + /* correlation id */ 4  /* topic count */);
        offsetsGroupedByTopic().forEach((topic, errorAndOffsetsMap) -> {
            intCount.add(shortStringLength(topic) + 4  /* partition count */);
            errorAndOffsetsMap.forEach((topicAndPartition, response) ->
                    intCount.add(4 + /* partition id */
                            2 + /* partition error */
                            4 + /* offset array length */
                            response.offsets.size() * 8 /* offset */));
        });
        return intCount.get();
    }


    public void writeTo(ByteBuffer buffer) {
        buffer.putInt(correlationId);
        buffer.putInt(offsetsGroupedByTopic().size()); // topic count;

        offsetsGroupedByTopic().forEach((topic,errorAndOffsetsMap)->{
                writeShortString(buffer, topic);
                buffer.putInt(errorAndOffsetsMap.size()); // partition count;
                errorAndOffsetsMap.forEach ((topicAndPartition,errorAndOffsets)->{
                    buffer.putInt(topicAndPartition.partition);
                    buffer.putShort(errorAndOffsets.error);
                    buffer.putInt(errorAndOffsets.offsets.size()); // offset array length;
                    errorAndOffsets.offsets.forEach(f->buffer.putLong(f));
            });
        });
    }

    @Override
    public String describe(Boolean details) {
        return this.toString();
    }
}
