package kafka.javaapi;

import kafka.log.TopicAndPartition;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @author zhoulf
 * @create 2017-12-19 16 20
 **/

public class OffsetCommitResponse {
    private kafka.api.OffsetCommitResponse underlying;

    public OffsetCommitResponse(kafka.api.OffsetCommitResponse underlying) {
        this.underlying = underlying;
    }

    public static OffsetCommitResponse readFrom(ByteBuffer buffer) {
        return new OffsetCommitResponse(kafka.api.OffsetCommitResponse.readFrom(buffer));
    }

    public Map<TopicAndPartition, Short> errors() {
        return underlying.commitStatus;
    }

    public boolean hasError() {
        return underlying.hasError();
    }

    public Short errorCode(TopicAndPartition topicAndPartition) {
        return underlying.commitStatus.get(topicAndPartition);
    }

}
