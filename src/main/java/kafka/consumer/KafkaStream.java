package kafka.consumer;

import kafka.message.MessageAndMetadata;
import kafka.serializer.Decoder;

import java.util.concurrent.BlockingQueue;

/**
 * @author zhoulf
 * @create 2017-12-14 45 13
 **/

public class KafkaStream<K, V> implements Iterable<MessageAndMetadata<K, V>> {
    private BlockingQueue<FetchedDataChunk> queue;
    Integer consumerTimeoutMs;
    private Decoder<K> keyDecoder;
    private Decoder<V> valueDecoder;
    public String clientId;
    private ConsumerIterator<K, V> iter ;

    public KafkaStream(BlockingQueue<FetchedDataChunk> queue, Integer consumerTimeoutMs, Decoder<K> keyDecoder, Decoder<V> valueDecoder, String clientId) {
        this.queue = queue;
        this.consumerTimeoutMs = consumerTimeoutMs;
        this.keyDecoder = keyDecoder;
        this.valueDecoder = valueDecoder;
        this.clientId = clientId;
        iter = new ConsumerIterator<K, V>(queue, consumerTimeoutMs, keyDecoder, valueDecoder, clientId);
    }

    /**
     * Create an iterator over messages in the stream.
     */
    public ConsumerIterator<K, V> iterator() {
        return iter;
    }

    /**
     * This method clears the queue being iterated during the consumer rebalancing. This is mainly
     * to reduce the number of duplicates received by the consumer
     */
    public void clear() {
        iter.clearCurrentChunk();
    }

    @Override
    public String toString() {
        return String.format("%s kafka stream", clientId);
    }
}
