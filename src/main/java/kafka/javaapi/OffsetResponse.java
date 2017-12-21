package kafka.javaapi;

import kafka.log.TopicAndPartition;

import java.util.List;

/**
 * @author zhoulf
 * @create 2017-12-19 17 20
 **/

public class OffsetResponse {
    private kafka.api.OffsetResponse underlying;

    public OffsetResponse(kafka.api.OffsetResponse underlying) {
        this.underlying = underlying;
    }

    public boolean hasError() {
        return underlying.hasError();
    }


    public Short errorCode(String topic, Integer partition) {
        return underlying.partitionErrorAndOffsets.get(new TopicAndPartition(topic, partition)).error;
    }


    public List<Long> offsets(String topic, Integer partition) {
        return underlying.partitionErrorAndOffsets.get(new TopicAndPartition(topic, partition)).offsets;
    }


    @Override
    public boolean equals(Object other) {
        return canEqual(other) && this.underlying.equals(((kafka.javaapi.OffsetResponse) other).underlying);
    }


    public boolean canEqual(Object other) {
        return other instanceof kafka.javaapi.OffsetResponse;
    }


    @Override
    public int hashCode() {
        return underlying.hashCode();
    }


    @Override
    public String toString() {
        return underlying.toString();
    }

}
