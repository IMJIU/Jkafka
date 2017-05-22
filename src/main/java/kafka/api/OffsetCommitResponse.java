package kafka.api;

import kafka.common.ErrorMapping;
import kafka.func.IntCount;
import kafka.func.Tuple;
import kafka.log.TopicAndPartition;
import kafka.utils.Utils;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Single constructor for both version 0 and 1 since they have the same format.
 */
public class OffsetCommitResponse extends RequestOrResponse {

    public static Short CurrentVersion = 1;
    public Map<TopicAndPartition, Short> commitStatus;
    public Integer correlationId = 0;
    //lazy
    public Map<String, Map<TopicAndPartition, Short>> commitStatusGroupedByTopic;

    public OffsetCommitResponse(Map<TopicAndPartition, Short> commitStatus, Integer correlationId) {
        this.commitStatus = commitStatus;
        this.correlationId = correlationId;
        commitStatusGroupedByTopic = Utils.groupByKey(commitStatus, t -> t.topic);
    }

    public static OffsetCommitResponse readFrom(ByteBuffer buffer) {
        Integer correlationId = buffer.getInt();
        Integer topicCount = buffer.getInt();
        List<Tuple<TopicAndPartition, Short>> pairs = Stream.iterate(1, n -> n + 1).limit(topicCount).flatMap(n -> {
            String topic = ApiUtils.readShortString(buffer);
            Integer partitionCount = buffer.getInt();
            return Stream.iterate(1, m -> m + 1).limit(partitionCount).map(m -> {
                Integer partitionId = buffer.getInt();
                Short error = buffer.getShort();
                return Tuple.of(new TopicAndPartition(topic, partitionId), error);
            });
        }).collect(Collectors.toList());
        return new OffsetCommitResponse(Utils.toMap(pairs), correlationId);
    }


    public boolean hasError() {
        return commitStatus.values().stream().anyMatch(errorCode -> errorCode != ErrorMapping.NoError);
    }

    public void writeTo(ByteBuffer buffer) {
        buffer.putInt(correlationId);
        buffer.putInt(commitStatusGroupedByTopic.size());
        commitStatusGroupedByTopic.forEach((topic, statusMap) -> {
            ApiUtils.writeShortString(buffer, topic);
            buffer.putInt(statusMap.size()); // partition count;
            statusMap.forEach((topicAndPartition, errorCode) -> {
                buffer.putInt(topicAndPartition.partition);
                buffer.putShort(errorCode);
            });
        });
    }

    @Override
    public Integer sizeInBytes() {
        final IntCount count = IntCount.of(0);
        commitStatusGroupedByTopic.forEach((topic, partitionStatus) -> {
            count.add(ApiUtils.shortStringLength(topic) +
                    4 + /* partition count */
                    partitionStatus.size() * (4 /* partition */ + 2 /* error code */));
        });
        return 4 + /* correlationId */
                4 + /* topic count */
                count.get();
    }

    @Override
    public String describe(Boolean details) {
        return this.toString();
    }

}

