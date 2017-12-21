package kafka.javaapi;

import kafka.utils.Sc;

import java.util.List;

/**
 * @author zhoulf
 * @create 2017-12-19 18 20
 **/

public class TopicMetadataResponse {
    private kafka.api.TopicMetadataResponse underlying;

    public TopicMetadataResponse(kafka.api.TopicMetadataResponse underlying) {
        this.underlying = underlying;
    }

    public Integer sizeInBytes() {
        return underlying.sizeInBytes();
    }

    public List<kafka.javaapi.TopicMetadata> topicsMetadata() {
        return Sc.map(underlying.topicsMetadata, t -> new TopicMetadata(t));
    }

    @Override
    public boolean equals(Object other) {
        return canEqual(other) && this.underlying.equals(((kafka.javaapi.TopicMetadataResponse) other).underlying);
    }

    public boolean canEqual(Object other) {
        return other instanceof kafka.javaapi.TopicMetadataResponse;
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
