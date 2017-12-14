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
}
