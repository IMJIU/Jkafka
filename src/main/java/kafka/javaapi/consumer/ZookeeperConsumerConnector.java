package kafka.javaapi.consumer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import kafka.common.MessageStreamsExistException;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.consumer.TopicFilter;
import kafka.serializer.Decoder;
import kafka.serializer.DefaultDecoder;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class handles the consumers interaction with zookeeper
 * <p>
 * Directories:
 * 1. Consumer id registry:
 * /consumers/<group_id>/ids<consumer_id> -> topic1,...topicN
 * A consumer has a unique consumer id within a consumer group. A consumer registers its id as an ephemeral znode
 * and puts all topics that it subscribes to as the value of the znode. The znode is deleted when the client is gone.
 * A consumer subscribes to event changes of the consumer id registry within its group.
 * <p>
 * The consumer id is picked up from configuration, instead of the sequential id assigned by ZK. Generated sequential
 * ids are hard to recover during temporary connection loss to ZK, since it's difficult for the client to figure out
 * whether the creation of a sequential znode has succeeded or not. More details can be found at
 * (http://wiki.apache.org/hadoop/ZooKeeper/ErrorHandling)
 * <p>
 * 2. Broker node registry:
 * /brokers/<0...N> --> { "host" : "port host",
 * "topics" : {"topic1": <"partition1" ... "partitionN">, ...,
 * "topicN": <"partition1" ... "partitionN"> } }
 * This is a list of all present broker brokers. A unique logical node id is configured on each broker node. A broker
 * node registers itself on start-up and creates a znode with the logical node id under /brokers. The value of the znode
 * is a JSON String that contains (1) the host name and the port the broker is listening to, (2) a list of topics that
 * the broker serves, (3) a list of logical partitions assigned to each topic on the broker.
 * A consumer subscribes to event changes of the broker node registry.
 * <p>
 * 3. Partition owner registry:
 * /consumers/<group_id>/owner/<topic>/<broker_id-partition_id> --> consumer_node_id
 * This stores the mapping before broker partitions and consumers. Each partition is owned by a unique consumer
 * within a consumer group. The mapping is reestablished after each rebalancing.
 * <p>
 * 4. Consumer offset tracking:
 * /consumers/<group_id>/offsets/<topic>/<broker_id-partition_id> --> offset_counter_value
 * Each consumer tracks the offset of the latest message consumed for each partition.
 */
// for testing only;
public class ZookeeperConsumerConnector implements ConsumerConnector {
    public ConsumerConfig config;
    public Boolean enableFetcher;

    public ZookeeperConsumerConnector(ConsumerConfig config, Boolean enableFetcher) {
        this.config = config;
        this.enableFetcher = enableFetcher;
    }

    private kafka.consumer.ZookeeperConsumerConnector underlying = new kafka.consumer.ZookeeperConsumerConnector(config, enableFetcher);
    private AtomicBoolean messageStreamCreated = new AtomicBoolean(false);

    public ZookeeperConsumerConnector(ConsumerConfig config) {
        this(config, true);
    }

    // for java client;
    public <K, V> Map<String, List<KafkaStream<K, V>>> createMessageStreams(
            Map<String, Integer> topicCountMap,
            Decoder<K> keyDecoder,
            Decoder<V> valueDecoder) {

        if (messageStreamCreated.getAndSet(true))
            throw new MessageStreamsExistException(this.getClass().getSimpleName() +
                    " can create message streams at most once", null);
        Map<String, Integer> scalaTopicCountMap = topicCountMap;
        Map<String, List<KafkaStream<K, V>>> scalaReturn = underlying.consume(scalaTopicCountMap, keyDecoder, valueDecoder);
        Map<String, List<KafkaStream<K, V>>> ret = Maps.newHashMap();
        scalaReturn.forEach((topic, streams) -> {
            List<KafkaStream<K, V>> javaStreamList = Lists.newArrayList();
            for (KafkaStream<K, V> stream : streams)
                javaStreamList.add(stream);
            ret.put(topic, javaStreamList);
        });
        return ret;
    }

    public Map<String, List<KafkaStream<byte[], byte[]>>> createMessageStreams(Map<String, Integer> topicCountMap) {
        return createMessageStreams(topicCountMap, new DefaultDecoder(), new DefaultDecoder());
    }

    public <K, V> List<KafkaStream<K, V>> createMessageStreamsByFilter(TopicFilter topicFilter, int numStreams, Decoder<K> keyDecoder, Decoder<V> valueDecoder) {
        return underlying.createMessageStreamsByFilter(topicFilter, numStreams, keyDecoder, valueDecoder);
    }

    public List<KafkaStream<byte[], byte[]>> createMessageStreamsByFilter(TopicFilter topicFilter, int numStreams) {
        return createMessageStreamsByFilter(topicFilter, numStreams, new DefaultDecoder(), new DefaultDecoder());
    }

    public List<KafkaStream<byte[], byte[]>> createMessageStreamsByFilter(TopicFilter topicFilter) {
        return createMessageStreamsByFilter(topicFilter, 1, new DefaultDecoder(), new DefaultDecoder());
    }

    public void commitOffsets() {
        underlying.commitOffsets(true);
    }

    @Override
    public void commitOffsets(boolean retryOnFailure) {

    }

    public void commitOffsets(Boolean retryOnFailure) {
        underlying.commitOffsets(retryOnFailure);
    }

    public void shutdown() {
        underlying.shutdown();
    }
}
