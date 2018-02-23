package kafka.message;

import kafka.serializer.Decoder;
import kafka.utils.Utils;

/**
 * @author zhoulf
 * @create 2017-12-14 47 13
 **/

public class MessageAndMetadata<K, V> {
    public String topic;
    public Integer partition;
    private Message rawMessage;
    public Long offset;
    public Decoder<K> keyDecoder;
    public Decoder<V> valueDecoder;

    public MessageAndMetadata(String topic, Integer partition, Message rawMessage, Long offset, Decoder<K> keyDecoder, Decoder<V> valueDecoder) {
        this.topic = topic;
        this.partition = partition;
        this.rawMessage = rawMessage;
        this.offset = offset;
        this.keyDecoder = keyDecoder;
        this.valueDecoder = valueDecoder;
    }

    /**
     * Return the decoded message key and payload
     */
    public K key() {
        if (rawMessage.key() == null)
            return null;
        else
            return keyDecoder.fromBytes(Utils.readBytes(rawMessage.key()));
    }

    public V message() {
        if (rawMessage.isNull()) return null;
        else return valueDecoder.fromBytes(Utils.readBytes(rawMessage.payload()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MessageAndMetadata<?, ?> that = (MessageAndMetadata<?, ?>) o;

        if (topic != null ? !topic.equals(that.topic) : that.topic != null) return false;
        if (partition != null ? !partition.equals(that.partition) : that.partition != null) return false;
        if (rawMessage != null ? !rawMessage.equals(that.rawMessage) : that.rawMessage != null) return false;
        if (offset != null ? !offset.equals(that.offset) : that.offset != null) return false;
        if (keyDecoder != null ? !keyDecoder.equals(that.keyDecoder) : that.keyDecoder != null) return false;
        return valueDecoder != null ? valueDecoder.equals(that.valueDecoder) : that.valueDecoder == null;
    }

    @Override
    public int hashCode() {
        int result = topic != null ? topic.hashCode() : 0;
        result = 31 * result + (partition != null ? partition.hashCode() : 0);
        result = 31 * result + (rawMessage != null ? rawMessage.hashCode() : 0);
        result = 31 * result + (offset != null ? offset.hashCode() : 0);
        result = 31 * result + (keyDecoder != null ? keyDecoder.hashCode() : 0);
        result = 31 * result + (valueDecoder != null ? valueDecoder.hashCode() : 0);
        return result;
    }
}
