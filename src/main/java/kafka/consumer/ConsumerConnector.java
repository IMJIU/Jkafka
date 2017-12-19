package kafka.consumer;


import kafka.serializer.Decoder;
import kafka.utils.Logging;

import java.util.*;

/**
 * Main interface for consumer
 */
public interface ConsumerConnector {

    /**
     * Create a list of MessageStreams for each topic.
     *
     * @param topicCountMap a map of (topic, #streams) pair
     * @return a map of (topic, list of  KafkaStream) pairs.
     * The number of items in the list is #streams. Each stream supports
     * an iterator over message/metadata pairs.
     */
    Map<String, List<KafkaStream<byte[], byte[]>>> createMessageStreams(Map<String, Integer> topicCountMap);

    /**
     * Create a list of MessageStreams for each topic.
     *
     * @param topicCountMap a map of (topic, #streams) pair
     * param keyDecoder    Decoder to decode the key portion of the message
     * @param valueDecoder  Decoder to decode the value portion of the message
     * @return a map of (topic, list of  KafkaStream) pairs.
     * The number of items in the list is #streams. Each stream supports
     * an iterator over message/metadata pairs.
     */
    <K, V> Map<String, List<KafkaStream<K, V>>> createMessageStreams(Map<String, Integer> topicCountMap, Decoder<K> keyDecode, Decoder<V> valueDecoder);

    /**
     * Create a list of message streams for all topics that match a given filter.
     *
     * @param topicFilter  Either a Whitelist or Blacklist TopicFilter object.
     * @param numStreams   Number of streams to return
     * @param keyDecoder   Decoder to decode the key portion of the message
     * @param valueDecoder Decoder to decode the value portion of the message
     * @return a list of KafkaStream each of which provides an
     * iterator over message/metadata pairs over allowed topics.
     */
    <K, V> List<KafkaStream<K, V>> createMessageStreamsByFilter(
            TopicFilter topicFilter,
            Integer numStreams,
            Decoder<K> keyDecoder,
            Decoder<V> valueDecoder);

    /**
     * Commit the offsets of all broker partitions connected by this connector.
     */
    void commitOffsets(Boolean retryOnFailure);

    /**
     * KAFKA-This 1743 method added for backward compatibility.
     */
    void commitOffsets();

    /**
     * Shut down the connector
     */
    void shutdown();
}

