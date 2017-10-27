package kafka.api;/**
 * Created by zhoulf on 2017/4/25.
 */

import kafka.common.ErrorMapping;
import kafka.func.Handler;
import kafka.func.Tuple;
import kafka.log.TopicAndPartition;
import kafka.utils.Utils;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author
 * @create 2017-04-25 38 17
 **/
public class ProducerResponse extends RequestOrResponse {
    public Integer correlationId;
    public Map<TopicAndPartition, ProducerResponseStatus> status;

    public ProducerResponse(Integer correlationId, Map<TopicAndPartition, ProducerResponseStatus> status) {
        this.correlationId = correlationId;
        this.status = status;
    }

    public void init() {

    }

    public final static Handler<ByteBuffer, ProducerResponse> readFrom = (buffer) -> {
        Integer correlationId = buffer.getInt();
        Integer topicCount = buffer.getInt();
        List<Tuple<TopicAndPartition, ProducerResponseStatus>> statusPairs =
                Stream.iterate(1, n -> n + 1).limit(topicCount).flatMap(n -> {
                    String topic = ApiUtils.readShortString(buffer);
                    Integer partitionCount = buffer.getInt();
                    List<Tuple<TopicAndPartition, ProducerResponseStatus>> list =
                            Stream.iterate(1, m -> m + 1).limit(partitionCount).map(p -> {
                                Integer partition = buffer.getInt();
                                short error = buffer.getShort();
                                long offset = buffer.getLong();
                                return Tuple.of(new TopicAndPartition(topic, partition),
                                        new ProducerResponseStatus(error, offset));
                            }).collect(Collectors.toList());
                    return list.stream();
                }).collect(Collectors.toList());
        return new ProducerResponse(correlationId, Utils.toMap(statusPairs));
    };


    /**
     * Partitions the status map into a map of maps (one for each topic).
     */
    private Map<String, Map<TopicAndPartition, ProducerResponseStatus>> statusGroupedByTopic =
            Utils.groupByKey(status, k -> k.topic);

    public Boolean hasError = status.values().stream().allMatch(e -> e.error != ErrorMapping.NoError);

    @Override
    public Integer sizeInBytes() {
        Map<String, Map<TopicAndPartition, ProducerResponseStatus>> groupedStatus = statusGroupedByTopic;
        AtomicInteger foldedTopics = new AtomicInteger(0);
        groupedStatus.entrySet().stream().forEach(currTopic -> {
            foldedTopics.set(foldedTopics.intValue() + ApiUtils.shortStringLength(currTopic.getKey()) +
                    currTopic.getValue().size() * (
                            4 + /* partition id */
                                    2 + /* error code */
                                    8 /* offset */
                    ));
        });
        return 4 + /* correlation id */
                4 + /* topic count */
                foldedTopics.intValue();
    }

    public void writeTo(ByteBuffer buffer) {
        Map<String, Map<TopicAndPartition, ProducerResponseStatus>> groupedStatus = statusGroupedByTopic;
        buffer.putInt(correlationId);
        buffer.putInt(groupedStatus.size()); // topic count;
        for (Map.Entry<String, Map<TopicAndPartition, ProducerResponseStatus>> topicStatus : groupedStatus.entrySet()) {
            String topic = topicStatus.getKey();
            Map<TopicAndPartition, ProducerResponseStatus> errorsAndOffsets = topicStatus.getValue();
            ApiUtils.writeShortString(buffer, topic);
            buffer.putInt(errorsAndOffsets.size()); // partition count;
            errorsAndOffsets.forEach((t, s) -> {
                buffer.putInt(t.partition);
                buffer.putShort(s.error);
                buffer.putLong(s.offset);
            });
        }
    }

    @Override
    public String describe(Boolean details) {
        return this.toString();
    }
}

