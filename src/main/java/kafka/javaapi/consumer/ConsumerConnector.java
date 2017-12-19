package kafka.javaapi.consumer;

import java.util.List;
import java.util.Map;

import kafka.consumer.KafkaStream;
import kafka.consumer.TopicFilter;
import kafka.serializer.Decoder;

 public interface ConsumerConnector {
    /**
     * Create a list of MessageStreams of type T for each topic.
     *
     * @param topicCountMap a map of (topic, #streams) pair
     * @param keyDecoder    a decoder that decodes the message key
     * @param valueDecoder  a decoder that decodes the message itself
     * @return a map of (topic, list of  KafkaStream) pairs.
     * The number of items in the list is #streams. Each stream supports
     * an iterator over message/metadata pairs.
     */
     <K, V> Map<String, List<KafkaStream<K, V>>>
    createMessageStreams(Map<String, Integer> topicCountMap, Decoder<K> keyDecoder, Decoder<V> valueDecoder);

     Map<String, List<KafkaStream<byte[], byte[]>>> createMessageStreams(Map<String, Integer> topicCountMap);

    /**
     * Create a list of MessageAndTopicStreams containing messages of type T.
     *
     * @param topicFilter  a TopicFilter that specifies which topics to
     *                     subscribe to (encapsulates a whitelist or a blacklist).
     * @param numStreams   the number of message streams to return.
     * @param keyDecoder   a decoder that decodes the message key
     * @param valueDecoder a decoder that decodes the message itself
     * @return a list of KafkaStream. Each stream supports an
     * iterator over its MessageAndMetadata elements.
     */
     <K, V> List<KafkaStream<K, V>>
    createMessageStreamsByFilter(TopicFilter topicFilter, int numStreams, Decoder<K> keyDecoder, Decoder<V> valueDecoder);

     List<KafkaStream<byte[], byte[]>> createMessageStreamsByFilter(TopicFilter topicFilter, int numStreams);

     List<KafkaStream<byte[], byte[]>> createMessageStreamsByFilter(TopicFilter topicFilter);

    /**
     * Commit the offsets of all broker partitions connected by this connector.
     */
     void commitOffsets();

     void commitOffsets(boolean retryOnFailure);

    /**
     * Shut down the connector
     */
     void shutdown();
}