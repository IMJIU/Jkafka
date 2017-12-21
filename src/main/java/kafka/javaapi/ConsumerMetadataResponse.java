package kafka.javaapi;

import kafka.cluster.Broker;

import java.nio.ByteBuffer;

/**
 * @author zhoulf
 * @create 2017-12-19 15 20
 **/

public class ConsumerMetadataResponse {
    private kafka.api.ConsumerMetadataResponse underlying;

    public ConsumerMetadataResponse(kafka.api.ConsumerMetadataResponse underlying) {
        this.underlying = underlying;
    }

    public Short errorCode() {
        return underlying.errorCode;
    }

    public Broker coordinator() {
        return underlying.coordinatorOpt.get();
    }

    public ConsumerMetadataResponse readFrom(ByteBuffer buffer) {
        return new ConsumerMetadataResponse(kafka.api.ConsumerMetadataResponse.readFrom(buffer));
    }

    @Override
    public boolean equals(Object other) {
        return canEqual(other) &&
                this.underlying.equals(((kafka.javaapi.ConsumerMetadataResponse) other).underlying);
    }

    public boolean canEqual(Object other) {
        return other instanceof kafka.javaapi.ConsumerMetadataResponse;

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

