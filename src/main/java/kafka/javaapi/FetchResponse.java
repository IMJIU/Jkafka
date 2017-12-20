package kafka.javaapi;

/**
 * @author zhoulf
 * @create 2017-12-19 16 20
 **/

public class FetchResponse {
    private kafka.api.FetchResponse underlying;

    public kafka.javaapi.message.ByteBufferMessageSet messageSet(String topic, Integer partition) {
        return new kafka.javaapi.message.ByteBufferMessageSet(underlying.messageSet(topic, partition).buffer);
    }

    public long highWatermark(String topic, Integer partition) {
        return underlying.highWatermark(topic, partition);
    }

    public boolean hasError() {
        return underlying.hasError();
    }

    public short errorCode(String topic, Integer partition) {
        return underlying.errorCode(topic, partition);
    }

    @Override
    public boolean equals(Object other) {
        return canEqual(other) && this.underlying.equals(((kafka.javaapi.FetchResponse) other).underlying);
    }


    public boolean canEqual(Object other) {
        return other instanceof kafka.javaapi.FetchResponse;
    }

    @Override
    public int hashCode() {
        return underlying.hashCode();
    }
}
