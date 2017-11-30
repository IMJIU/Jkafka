package kafka.producer;

/**
 * @author zhoulf
 * @create 2017-11-30 22 15
 **/

/**
 * A topic, key, and value.
 * If a partition key is provided it will  @Override the key for the purpose of partitioning but will not be stored.
 */
public class KeyedMessage<K, V> {
    public String topic;
    public K key;
    public Object partKey;
    public V message;

    public KeyedMessage(String topic, K key, Object partKey, V message) {
        this.topic = topic;
        this.key = key;
        this.partKey = partKey;
        this.message = message;
        if (topic == null)
            throw new IllegalArgumentException("Topic cannot be null.");
    }


    public KeyedMessage(String topic, V message) {
        this(topic, null, null, message);
    }

    public KeyedMessage(String topic, K key, V message) {
        this(topic, key, key, message);
    }

    public Object partitionKey() {
        if (partKey != null)
            return partKey;
        else if (hasKey())
            return key;
        else
            return null;
    }

    public boolean hasKey() {
        return key != null;
    }
}
