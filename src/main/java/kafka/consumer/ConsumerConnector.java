package kafka.consumer;


import java.util.*;

/**
 *  Main interface for consumer
 */
public interface ConsumerConnector {

        /**
         *  Create a list of MessageStreams for each topic.
         *
         *  @param topicCountMap  a map of (topic, #streams) pair
         *  @return a map of (topic, list of  KafkaStream) pairs.
         *          The number of items in the list is #streams. Each stream supports
         *          an iterator over message/metadata pairs.
         */
    Map<String, List<KafkaStream<Byte[],Byte[]>>> createMessageStreams(Map<String,Integer> topicCountMap);

        /**
         *  Create a list of MessageStreams for each topic.
         *
         *  @param topicCountMap  a map of (topic, #streams) pair
         *  @param keyDecoder Decoder to decode the key portion of the message
         *  @param valueDecoder Decoder to decode the value portion of the message
         *  @return a map of (topic, list of  KafkaStream) pairs.
         *          The number of items in the list is #streams. Each stream supports
         *          an iterator over message/metadata pairs.
         */
        <K,V> Map<String,List<KafkaStream<K,V>>> createMessageStreams<K,V>(Map topicCountMap<String,Integer>,
        Decoder keyDecoder<K>,
        Decoder valueDecoder<V>);

        /**
         *  Create a list of message streams for all topics that match a given filter.
         *
         *  @param topicFilter Either a Whitelist or Blacklist TopicFilter object.
         *  @param numStreams Number of streams to return
         *  @param keyDecoder Decoder to decode the key portion of the message
         *  @param valueDecoder Decoder to decode the value portion of the message
         *  @return a list of KafkaStream each of which provides an
         *          iterator over message/metadata pairs over allowed topics.
         */
        void createMessageStreamsByFilter<K,V>(TopicFilter topicFilter,
        Integer numStreams = 1,
        Decoder keyDecoder<K> = new DefaultDecoder(),
        Decoder valueDecoder<V> = new DefaultDecoder());
        : Seq<KafkaStream<K,V>>

        /**
         *  Commit the offsets of all broker partitions connected by this connector.
         */
        void commitOffsets(Boolean retryOnFailure);

        /**
         * KAFKA-This 1743 method added for backward compatibility.
         */
        void commitOffsets;

        /**
         *  Shut down the connector
         */
        void shutdown();
        }

        object Consumer extends Logging {
        /**
         *  Create a ConsumerConnector
         *
         *  @param config  at the minimum, need to specify the groupid of the consumer and the zookeeper
         *                 connection string zookeeper.connect.
         */
       public ConsumerConnector  void create(ConsumerConfig config) {
        val consumerConnect = new ZookeeperConsumerConnector(config);
        consumerConnect;
        }

        /**
         *  Create a ConsumerConnector
         *
         *  @param config  at the minimum, need to specify the groupid of the consumer and the zookeeper
         *                 connection string zookeeper.connect.
         */
       public void createJavaConsumerConnector(ConsumerConfig config): kafka.javaapi.consumer.ConsumerConnector = {
        val consumerConnect = new kafka.javaapi.consumer.ZookeeperConsumerConnector(config);
        consumerConnect;
        }
        }
