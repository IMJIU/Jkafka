package kafka.utils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import kafka.admin.AdminUtils;
import kafka.api.*;
import kafka.cluster.Partition;
import kafka.cluster.Replica;
import kafka.common.Topic;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.func.Action;
import kafka.func.Fun;
import kafka.func.IntCount;
import kafka.func.Tuple;
import kafka.log.CleanerConfig;
import kafka.log.LogConfig;
import kafka.log.LogManager;
import kafka.log.TopicAndPartition;
import kafka.message.ByteBufferMessageSet;
import kafka.message.CompressionCodec;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import kafka.producer.*;
import kafka.serializer.Encoder;
import kafka.serializer.StringEncoder;
import kafka.server.BrokerState;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import org.I0Itec.zkclient.ZkClient;
import org.junit.Assert;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.*;

import static org.apache.kafka.common.utils.Utils.formatAddress;
import static kafka.message.CompressionCodec.*;

/**
 * Created by Administrator on 2017/3/26.
 */
public class TestUtils {
    public static final Logging log = Logging.getLogger(TestUtils.class.getName());
    public final static String IoTmpDir = "f:\\temp\\";
    public final static String Letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    public final static String Digits = "0123456789";
    public final static String LettersAndDigits = Letters + Digits;

    /* A consistent random number generator to make tests repeatable */
    public static Random seededRandom = new Random(192348092834L);
    public static Random random = new Random();

    public static void checkEquals(ByteBuffer b1, ByteBuffer b2) {
        Assert.assertEquals("Buffers should have equal length", b1.limit() - b1.position(), b2.limit() - b2.position());
        for (int i = 0; i < b1.limit() - b1.position(); i++)
            Assert.assertEquals("byte " + i + " byte not equal.", b1.get(b1.position() + i), b2.get(b1.position() + i));
    }

    /**
     * Create a temporary file
     */
    public static File tempFile() {
        File f = null;
        try {
            f = File.createTempFile("kafka", ".tmp");
            f.deleteOnExit();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return f;
    }

    public static void assertEquals(String msg, Iterator<MessageAndOffset> expected, Iterator<MessageAndOffset> actual) {
        checkEquals(expected, actual);
    }

    /**
     * Throw an exception if the two iterators are of differing lengths or contain
     * different messages on their Nth element
     */
    public static <T> void checkEquals(Iterator<T> expected, Iterator<T> actual) {
        int length = 0;
        while (expected.hasNext() && actual.hasNext()) {
            length += 1;
//            System.out.println(expected.next());
//            System.out.println(actual.next());
            Assert.assertEquals(expected.next(), actual.next());
        }

        // check if the expected iterator is longer
        if (expected.hasNext()) {
            int length1 = length;
            while (expected.hasNext()) {
                expected.next();
                length1 += 1;
            }
            Assert.assertFalse("Iterators have uneven length-- first has more: " + length1 + " > " + length, true);
        }

        // check if the actual iterator was longer
        if (actual.hasNext()) {
            int length2 = length;
            while (actual.hasNext()) {
                actual.next();
                length2 += 1;
            }
            Assert.assertFalse("Iterators have uneven length-- second has more: " + length2 + " > " + length, true);
        }
    }

    @SuppressWarnings("unchecked")
    public static Iterator<Message> getMessageIterator(final Iterator<MessageAndOffset> iterator) {
        return new IteratorTemplate<Message>() {
            @Override
            protected Message makeNext() {
                if (iterator.hasNext())
                    return iterator.next().message;
                else
                    return allDone();
            }
        };
    }

    public static ByteBufferMessageSet singleMessageSet(byte[] payload) {
        byte[] key = null;
        return new ByteBufferMessageSet(CompressionCodec.NoCompressionCodec, new Message(payload, key));
    }


    public static void print(Iterator<MessageAndOffset> iterator) {
        System.out.println("================");
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }
        System.out.println("================");
    }


    public static void writeNonsenseToFile(File fileName, Long position, Integer size) throws Exception {
        RandomAccessFile file = new RandomAccessFile(fileName, "rw");
        file.seek(position);
        for (int i = 0; i < size; i++)
            file.writeByte(random.nextInt(255));
        file.close();
    }

    /**
     * Create a temporary directory
     */
    public static File tempDir() {
        File f = new File(IoTmpDir, "kafka-" + random.nextInt(1000000));
        f.mkdirs();
        f.deleteOnExit();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                Utils.rm(f);
            }
        });
        return f;
    }
//

    /**
     * Choose a number of random available ports
     */
    public static List<Integer> choosePorts(Integer count) {
        try {
            List<ServerSocket> socketList = Lists.newArrayList();
            for (int i = 0; i < count; i++) {
                socketList.add(new ServerSocket(0));
            }
            List<Integer> ports = Sc.map(socketList, s -> s.getLocalPort());
            for (ServerSocket s : socketList) {
                s.close();
            }
            return ports;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    //
//    /**
//     * Choose an available port
//     */
    public static Integer choosePort() {
        return choosePorts(1).get(0);
    }
//
//    public void  tempTopic(): String = "testTopic" + random.nextInt(1000000);
//

    /**
     * Create a temporary relative directory
     */
    public static File tempRelativeDir(String parent) {
        File f = new File(parent, "kafka-" + random.nextInt(1000000));
        f.mkdirs();
        f.deleteOnExit();
        return f;
    }
//
//    /**
//     * Create a temporary file
//     */
//    public void  tempFile(): File = {
//        val f = File.createTempFile("kafka", ".tmp");
//        f.deleteOnExit();
//        f;
//    }
//
//    /**
//     * Create a temporary file and return an open file channel for this file
//     */
//    public void  tempChannel(): FileChannel = new RandomAccessFile(tempFile(), "rw").getChannel();
//

    /**
     * Create a kafka server instance with appropriate test settings
     * USING THIS IS A SIGN YOU ARE NOT WRITING A REAL UNIT TEST
     *
     * @param config The configuration of the server
     */
    public static KafkaServer createServer(KafkaConfig config) {
        return createServer(config, null);
    }

    public static KafkaServer createServer(KafkaConfig config, Time time) {
        if (time == null) {
            time = Time.get();
        }
        KafkaServer server = new KafkaServer(config, time);
        server.startup();
        return server;
    }


    /**
     * Create a test config for the given node id
     */
    public static List<Properties> createBrokerConfigs(Integer numConfigs, Boolean enableControlledShutdown) {
        enableControlledShutdown = enableControlledShutdown == null ? true : enableControlledShutdown;
        List<Integer> ports = choosePorts(numConfigs);
        List<Properties> properties = Lists.newArrayList();
        for (int node = 0; node < ports.size(); node++) {
            properties.add(createBrokerConfig(node, ports.get(node), enableControlledShutdown));
        }
        return properties;
    }

    public static String getBrokerListStrFromConfigs(List<KafkaConfig> configs) {
        return Sc.mkString(Sc.map(configs, c -> formatAddress(c.hostName, c.port)), ",");
    }


    /**
     * Create a test config for the given node id
     */
    public static Properties createBrokerConfig(Integer nodeId, Integer port, Boolean enableControlledShutdown) {
        port = port == null ? choosePort() : port;
        enableControlledShutdown = enableControlledShutdown == null ? true : enableControlledShutdown;
        Properties props = new Properties();
        props.put("broker.id", nodeId.toString());
        props.put("host.name", "localhost");
        props.put("port", port.toString());
        props.put("log.dir", TestUtils.tempDir().getAbsolutePath());
        props.put("zookeeper.connect", TestZKUtils.zookeeperConnect());
        props.put("replica.socket.timeout.ms", "1500");
        props.put("controlled.shutdown.enable", enableControlledShutdown.toString());
        return props;
    }
//

    /**
     * Create a topic in zookeeper.
     * Wait until the leader is elected and the metadata is propagated to all brokers.
     * Return the leader for each partition.
     */
    public static Map<Integer, Optional<Integer>> createTopic(ZkClient zkClient,
                                                              String topic,
                                                              Integer numPartitions,
                                                              Integer replicationFactor,
                                                              List<KafkaServer> servers,
                                                              Properties topicConfig) {
        if (numPartitions == null) {
            numPartitions = 1;
        }
        if (replicationFactor == null) {
            replicationFactor = 1;
        }
        if (topicConfig == null) {
            topicConfig = new Properties();
        }
        // create topic;
        AdminUtils.createTopic(zkClient, topic, numPartitions, replicationFactor, topicConfig);
        // wait until the update metadata request for new topic reaches all servers;
        return Sc.toMap(Sc.itToList(0, numPartitions, i -> {
            TestUtils.waitUntilMetadataIsPropagated(servers, topic, i);
            return Tuple.of(i, TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, i, null, null, null));
        }));
    }
//

    /**
     * Create a topic in zookeeper using a customized replica assignment.
     * Wait until the leader is elected and the metadata is propagated to all brokers.
     * Return the leader for each partition.
     */
    public static Map<Integer, Optional<Integer>> createTopic(ZkClient zkClient, String topic, Map<Integer, List<Integer>> partitionReplicaAssignment,
                                                              List<KafkaServer> servers) {
        // create topic;
        AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, partitionReplicaAssignment);
        // wait until the update metadata request for new topic reaches all servers;
        return Sc.mapToMap(partitionReplicaAssignment.keySet(), i -> {
            TestUtils.waitUntilMetadataIsPropagated(servers, topic, i);
            return Tuple.of(i, TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, i, null, null, null));
        });
    }
//

    /**
     * Create a test config for a consumer
     */
    public static Properties createConsumerProperties(String zkConnect, String groupId, String consumerId) {
        return createConsumerProperties(zkConnect, groupId, consumerId, null);
    }

    public static Properties createConsumerProperties(String zkConnect, String groupId, String consumerId,
                                                      Long consumerTimeout) {
        if (consumerTimeout == null) consumerTimeout = -1L;
        Properties props = new Properties();
        props.put("zookeeper.connect", zkConnect);
        props.put("group.id", groupId);
        props.put("consumer.id", consumerId);
        props.put("consumer.timeout.ms", consumerTimeout.toString());
        props.put("zookeeper.session.timeout.ms", "6000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        props.put("rebalance.max.retries", "4");
        props.put("auto.offset.reset", "smallest");
        props.put("num.consumer.fetchers", "2");
        return props;
    }
//
//    /**
//     * Wrap the message in a message set
//     * @param payload The bytes of the message
//     */
//    public void  singleMessageSet(Array payload<Byte>, CompressionCodec codec = NoCompressionCodec, Array key<Byte> = null) =
//            new ByteBufferMessageSet(compressionCodec = codec, messages = new Message(payload, key));
//

    /**
     * Generate an array of random bytes
     *
     * @param numBytes The size of the array
     */
    public static byte[] randomBytes(Integer numBytes) {
        byte[] bytes = new byte[numBytes];
        seededRandom.nextBytes(bytes);
        return bytes;
    }
//

    /**
     * Generate a random string of letters and digits of the given length
     *
     * @param len The length of the string
     * @return The random string
     */
    public static String randomString(Integer len) {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < len; i++)
            b.append(LettersAndDigits.charAt(seededRandom.nextInt(LettersAndDigits.length())));
        return b.toString();
    }
//
//    /**
//     * Check that the buffer content from buffer.position() to buffer.limit() is equal
//     */
//    public void  checkEquals(ByteBuffer b1, ByteBuffer b2) {
//        Assert.assertEquals("Buffers should have equal length", b1.limit - b1.position, b2.limit - b2.position);
//        for(i <- 0 until b1.limit - b1.position)
//        Assert.assertEquals("byte " + i + " byte not equal.", b1.get(b1.position + i), b2.get(b1.position + i));
//    }
//
//    /**
//     * Throw an exception if the two iterators are of differing lengths or contain
//     * different messages on their Nth element
//     */
//    public void  checkEquals<T](Iterator expected[T>, Iterator actual[T]) {
//        var length = 0;
//        while(expected.hasNext && actual.hasNext) {
//            length += 1;
//            Assert.assertEquals(expected.next, actual.next);
//        }
//
//        // check if the expected iterator is longer;
//        if (expected.hasNext) {
//            var length1 = length;
//            while (expected.hasNext) {
//                expected.next;
//                length1 += 1;
//            }
//            assertFalse("Iterators have uneven length-- first has more: "+length1 + " > " + length, true);
//        }
//
//        // check if the actual iterator was longer;
//        if (actual.hasNext) {
//            var length2 = length;
//            while (actual.hasNext) {
//                actual.next;
//                length2 += 1;
//            }
//            assertFalse("Iterators have uneven length-- second has more: "+length2 + " > " + length, true);
//        }
//    }
//
//    /**
//     *  Throw an exception if an iterable has different length than expected
//     *
//     */
//    public void  checkLength<T](Iterator s1[T>, Integer expectedLength) {
//        var n = 0;
//        while (s1.hasNext) {
//            n+=1;
//            s1.next;
//        }
//        Assert.assertEquals(expectedLength, n);
//    }
//
//    /**
//     * Throw an exception if the two iterators are of differing lengths or contain
//     * different messages on their Nth element
//     */
//    public void  checkEquals<T](java s1.util.Iterator[T>, java s2.util.Iterator[T]) {
//        while(s1.hasNext && s2.hasNext);
//            Assert.assertEquals(s1.next, s2.next);
//        assertFalse("Iterators have uneven length--first has more", s1.hasNext);
//        assertFalse("Iterators have uneven length--second has more", s2.hasNext);
//    }
//
//    public void  stackedIterator<T](Iterator s[T>*): Iterator[T] = {
//        new Iterator[T] {
//            var Iterator cur[T] = null;
//            val topIterator = s.iterator;
//
//            public void  hasNext() : Boolean = {
//            while (true) {
//                if (cur == null) {
//                    if (topIterator.hasNext)
//                        cur = topIterator.next;
//                    else;
//                        return false;
//                }
//                if (cur.hasNext)
//                    return true;
//                cur = null;
//            }
//            // should never reach her;
//            throw new RuntimeException("should not reach here");
//            }
//
//            public void  next() : T = cur.next;
//        }
//    }
//
//    /**
//     * Create a hexidecimal string for the given bytes
//     */
//    public void  hexString(Array bytes<Byte>): String = hexString(ByteBuffer.wrap(bytes));
//
//    /**
//     * Create a hexidecimal string for the given bytes
//     */
//    public void  hexString(ByteBuffer buffer): String = {
//        val builder = new StringBuilder("0x");
//        for(i <- 0 until buffer.limit)
//        builder.append(String.format("%x", Integer.valueOf(buffer.get(buffer.position + i))))
//        builder.toString;
//    }
//

    /**
     * Create a producer with a few pre-configured properties.
     * If certain properties need to be overridden, they can be provided in producerProps.
     */
    public static <K, V> Producer<K, V> createProducer(String brokerList,
                                                       String encoder,
                                                       String keyEncoder,
                                                       String partitioner,
                                                       Properties producerProps) {
//        String encoder = classOf<DefaultEncoder>.getName,
//                String keyEncoder = classOf<DefaultEncoder>.getName,
//                String partitioner = classOf<DefaultPartitioner>.getName,
        Properties props = getProducerConfig(brokerList);

        //override any explicitly specified properties;
        if (producerProps != null)
            props.putAll(producerProps);

        props.put("serializer.class", encoder);
        props.put("key.serializer.class", keyEncoder);
        props.put("partitioner.class", partitioner);
        return new Producer(new ProducerConfig(props));
    }

    /**
     * Create a (new) producer with a few pre-configured properties.
     */
//    public void  createNewProducer(String brokerList,
//                          Integer acks = -1,
//                          Long metadataFetchTimeout = 3000L,
//                          Boolean blockOnBufferFull = true,
//                          Long bufferSize = 1024L * 1024L,
//                          Integer retries = 0) : KafkaProducer<Array[Byte],Array[Byte]> = {
//
//        Properties producerProps = new Properties();
//        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
//        producerProps.put(ProducerConfig.ACKS_CONFIG, acks.toString);
//        producerProps.put(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG, metadataFetchTimeout.toString);
//        producerProps.put(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, blockOnBufferFull.toString);
//        producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferSize.toString);
//        producerProps.put(ProducerConfig.RETRIES_CONFIG, retries.toString);
//        producerProps.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "100");
//        producerProps.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, "200");
//        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
//        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
//        return new KafkaProducer<Array[Byte],Array[Byte]>(producerProps);
//    }

    /**
     * Create a default producer config properties map with the given metadata broker list
     */
    public static Properties getProducerConfig(String brokerList) {
        Properties props = new Properties();
        props.put("metadata.broker.list", brokerList);
        props.put("message.send.max.retries", "5");
        props.put("retry.backoff.ms", "1000");
        props.put("request.timeout.ms", "2000");
        props.put("request.required.acks", "-1");
        props.put("send.buffer.bytes", "65536");
        props.put("connect.timeout.ms", "100000");
        props.put("reconnect.interval", "10000");

        return props;
    }

    //
    public static Properties getSyncProducerConfig(Integer port) {
        Properties props = new Properties();
        props.put("host", "localhost");
        props.put("port", port.toString());
        props.put("request.timeout.ms", "500");
        props.put("request.required.acks", "1");
        props.put("serializer.class", StringEncoder.class.getName());
        return props;
    }
//
//    public void  updateConsumerOffset(config : ConsumerConfig, path : String, offset : Long) = {
//        val zkClient = new ZkClient(config.zkConnect, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs, ZKStringSerializer);
//        ZkUtils.updatePersistentPath(zkClient, path, offset.toString);
//
//    }
//
//    public void  getMessageIterator(Iterator iter<MessageAndOffset]): Iterator[Message> = {
//        new IteratorTemplate<Message> {
//            override public void  makeNext(): Message = {
//            if (iter.hasNext)
//                return iter.next.message;
//            else;
//                return allDone();
//            }
//        }
//    }
//
//    public void  createBrokersInZk(ZkClient zkClient, Seq ids<Int]): Seq[Broker> = {
//        val brokers = ids.map(id => new Broker(id, "localhost", 6667));
//        brokers.foreach(b => ZkUtils.registerBrokerInZk(zkClient, b.id, b.host, b.port, 6000, jmxPort = -1))
//        brokers;
//    }
//
//    public void  deleteBrokersInZk(ZkClient zkClient, Seq ids<Int]): Seq[Broker> = {
//        val brokers = ids.map(id => new Broker(id, "localhost", 6667));
//        brokers.foreach(b => ZkUtils.deletePath(zkClient, ZkUtils.BrokerIdsPath + "/" + b))
//        brokers;
//    }
//
//    public void  getMsgStrings Integer n): Seq<String> = {
//        val buffer = new ListBuffer<String>;
//        for (i <- 0 until  n)
//        buffer += ("msg" + i);
//        buffer;
//    }
//

    /**
     * Create a wired format request based on simple basic information
     */
    public ProducerRequest produceRequest(String topic,
                                          Integer partition,
                                          ByteBufferMessageSet message) {
        return produceRequest(topic, partition, message,
                SyncProducerConfig.DefaultRequiredAcks.intValue(),
                SyncProducerConfig.DefaultAckTimeoutMs, 0,
                SyncProducerConfig.DefaultClientId);
    }

    public ProducerRequest produceRequest(String topic,
                                          Integer partition,
                                          ByteBufferMessageSet message,
                                          Integer acks,
                                          Integer timeout,
                                          Integer correlationId,
                                          String clientId) {
        return produceRequestWithAcks(Lists.newArrayList(topic), Lists.newArrayList(partition), message, acks, timeout, correlationId, clientId);
    }

    public ProducerRequest produceRequestWithAcks(List<String> topics, List<Integer> partitions, ByteBufferMessageSet message) {
        return produceRequestWithAcks(topics, partitions, message,
                SyncProducerConfig.DefaultRequiredAcks.intValue(),
                SyncProducerConfig.DefaultAckTimeoutMs,
                0, SyncProducerConfig.DefaultClientId);
    }

    public ProducerRequest produceRequestWithAcks(List<String> topics,
                                                  List<Integer> partitions,
                                                  ByteBufferMessageSet message,
                                                  Integer acks,
                                                  Integer timeout,
                                                  Integer correlationId,
                                                  String clientId) {
        List<Tuple<TopicAndPartition, ByteBufferMessageSet>> list = Lists.newArrayList();
        for (String topic : topics) {
            list.addAll(Sc.map(partitions, partition -> Tuple.of(new TopicAndPartition(topic, partition), message)));
        }
        Map<TopicAndPartition, ByteBufferMessageSet> data = Sc.toMap(list);
        return new ProducerRequest(correlationId, clientId, acks.shortValue(), timeout, data);
    }

    public void makeLeaderForPartition(ZkClient zkClient, String topic,
                                       Map<Integer, Integer> leaderPerPartitionMap,
                                       Integer controllerEpoch) {
        leaderPerPartitionMap.forEach((partition, leader) -> {
            try {
                Optional<LeaderAndIsr> currentLeaderAndIsrOpt = ZkUtils.getLeaderAndIsrForPartition(zkClient, topic, partition);
                LeaderAndIsr newLeaderAndIsr = null;
                if (!currentLeaderAndIsrOpt.isPresent())
                    newLeaderAndIsr = new LeaderAndIsr(leader, Lists.newArrayList(leader));
                else {
                    newLeaderAndIsr = currentLeaderAndIsrOpt.get();
                    newLeaderAndIsr.leader = leader;
                    newLeaderAndIsr.leaderEpoch += 1;
                    newLeaderAndIsr.zkVersion += 1;
                }
                ZkUtils.updatePersistentPath(zkClient, ZkUtils.getTopicPartitionLeaderAndIsrPath(topic, partition),
                        ZkUtils.leaderAndIsrZkData(newLeaderAndIsr, controllerEpoch));
            } catch (Throwable oe) {
                log.error(String.format("Error while electing leader for partition <%s,%d>", topic, partition), oe);
            }
        });
    }


    /**
     * If neither oldLeaderOpt nor newLeaderOpt is defined, wait until the leader of a partition is elected.
     * If oldLeaderOpt is defined, it waits until the new leader is different from the old leader.
     * If newLeaderOpt is defined, it waits until the new leader becomes the expected new leader.
     *
     * @return The new leader or assertion failure if timeout is reached.
     */
    public static Optional<Integer> waitUntilLeaderIsElectedOrChanged(ZkClient zkClient, String topic,
                                                                      Integer partition, Long timeoutMs,
                                                                      Optional<Integer> oldLeaderOpt,
                                                                      Optional<Integer> newLeaderOpt) {
        if (timeoutMs == null) {
            timeoutMs = 5000L;
        }
        if (oldLeaderOpt == null) {
            oldLeaderOpt = Optional.empty();
        }
        if (newLeaderOpt == null) {
            newLeaderOpt = Optional.empty();
        }

        Prediction.require(!(oldLeaderOpt.isPresent() && newLeaderOpt.isPresent()), "Can't define both the old and the new leader");
        long startTime = System.currentTimeMillis();
        boolean isLeaderElectedOrChanged = false;

        log.trace(String.format("Waiting for leader to be elected or changed for partition <%s,%d>, older leader is %s, new leader is %s",
                topic, partition, oldLeaderOpt, newLeaderOpt));

        Optional<Integer> leader = Optional.empty();
        while (!isLeaderElectedOrChanged && System.currentTimeMillis() < startTime + timeoutMs) {
            // check if leader is elected;
            leader = ZkUtils.getLeaderForPartition(zkClient, topic, partition);
            if (leader.isPresent()) {
                Integer l = leader.get();
                if (newLeaderOpt.isPresent() && newLeaderOpt.get() == l) {
                    log.trace(String.format("Expected new leader %d is elected for partition <%s,%d>", l, topic, partition));
                    isLeaderElectedOrChanged = true;
                } else if (oldLeaderOpt.isPresent() && oldLeaderOpt.get() != l) {
                    log.trace(String.format("Leader for partition <%s,%d> is changed from %d to %d", topic, partition, oldLeaderOpt.get(), l));
                    isLeaderElectedOrChanged = true;
                } else if (!oldLeaderOpt.isPresent()) {
                    log.trace(String.format("Leader %d is elected for partition <%s,%d>", l, topic, partition));
                    isLeaderElectedOrChanged = true;
                } else {
                    log.trace(String.format("Current leader for partition <%s,%d> is %d", topic, partition, l));
                }
            } else {
                log.trace(String.format("Leader for partition <%s,%d> is not elected yet", topic, partition));
            }
            try {
                Thread.sleep(Math.min(timeoutMs, 100L));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        if (!isLeaderElectedOrChanged)
            log.error(String.format("Timing out after %d ms since leader is not elected or changed for partition <%s,%d>", timeoutMs, topic, partition));

        return leader;
    }
//

    /**
     * Execute the given block. If it throws an assert error, retry. Repeat
     * until no error is thrown or the time limit ellapses
     */
    public static void retry(Long maxWaitMs, Action block) {
        Long wait = 1L;
        Long startTime = System.currentTimeMillis();
        while (true) {
            try {
                block.invoke();
                return;
            } catch (AssertionError e) {
                Long ellapsed = System.currentTimeMillis() - startTime;
                if (ellapsed > maxWaitMs) {
                    throw e;
                } else {
                    System.out.println("Attempt failed, sleeping for " + wait + ", and then retrying.");
                    try {
                        Thread.sleep(wait);
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }
                    wait += Math.min(wait, 1000);
                }
            }
        }
    }

    public static Boolean waitUntilTrue(Fun<Boolean> condition, String msg) {
        return waitUntilTrue(condition, msg, 5000L);
    }

    /**
     * Wait until the given condition is true or throw an exception if the given wait time elapses.
     */
    public static Boolean waitUntilTrue(Fun<Boolean> condition, String msg, Long waitTime) {
        long startTime = System.currentTimeMillis();
        while (true) {
            if (condition.invoke())
                return true;
            if (System.currentTimeMillis() > startTime + waitTime)
                Assert.fail(msg);
            try {
                Thread.sleep(Math.min(waitTime, 100L));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        // should never hit here;
//        throw new RuntimeException("unexpected error");
    }

    //
    public Boolean isLeaderLocalOnBroker(String topic, Integer partitionId, KafkaServer server) {
        Optional<Partition> partitionOpt = server.replicaManager.getPartition(topic, partitionId);
        if (partitionOpt.isPresent()) {
            Optional<Replica> replicaOpt = partitionOpt.get().leaderReplicaIfLocal();
            if (replicaOpt.isPresent()) {
                return true;
            }
            return false;
        } else {
            return false;
        }
    }

    //
    public ByteBuffer createRequestByteBuffer(RequestOrResponse request) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(request.sizeInBytes() + 2);
        byteBuffer.putShort(request.requestId.get());
        request.writeTo(byteBuffer);
        byteBuffer.rewind();
        return byteBuffer;
    }
//
//

    /**
     * Wait until a valid leader is propagated to the metadata cache in each broker.
     * It assumes that the leader propagated to each broker is the same.
     *
     * @param servers   The list of servers that the metadata should reach to
     * @param topic     The topic name
     * @param partition The partition Id
     *                  param timeout   The amount of time waiting on this condition before assert to fail
     * @return The leader of the partition.
     */
    public static Integer waitUntilMetadataIsPropagated(List<KafkaServer> servers, String topic, Integer partition) {
        return waitUntilMetadataIsPropagated(servers, topic, partition, 5000L);
    }

    public static Integer waitUntilMetadataIsPropagated(List<KafkaServer> servers, String topic, Integer partition, final Long timeout) {
        IntCount leader = IntCount.of(-1);
        TestUtils.waitUntilTrue(() ->
                        Sc.foldBooleanAll(servers, true, s -> {
                            Optional<PartitionStateInfo> partitionStateOpt = s.apis.metadataCache.getPartitionInfo(topic, partition);
                            if (partitionStateOpt.isPresent()) {
                                PartitionStateInfo partitionState = partitionStateOpt.get();
                                leader.set(partitionState.leaderIsrAndControllerEpoch.leaderAndIsr.leader);
                                return Request.isValidBrokerId(leader.get());
                            } else {
                                return false;
                            }
                        }),
                String.format("Partition <%s,%d> metadata not propagated after %d ms", topic, partition, timeout),
                timeout);
        return leader.get();
    }

    //
//    public void  writeNonsenseToFile(File fileName, Long position, Integer size) {
//        val file = new RandomAccessFile(fileName, "rw");
//        file.seek(position);
//        for(i <- 0 until size)
//        file.writeByte(random.nextInt(255));
//        file.close();
//    }
//
    public static void appendNonsenseToFile(File fileName, Integer size) throws IOException {
        FileOutputStream file = new FileOutputStream(fileName, true);
        for (int i = 0; i < size; i++)
            file.write(random.nextInt(255));
        file.close();
    }
//
//    public void  checkForPhantomInSyncReplicas(ZkClient zkClient, String topic, Integer partitionToBeReassigned, Seq assignedReplicas<Int>) {
//        val inSyncReplicas = ZkUtils.getInSyncReplicasForPartition(zkClient, topic, partitionToBeReassigned);
//        // in sync replicas should not have any replica that is not in the new assigned replicas;
//        val phantomInSyncReplicas = inSyncReplicas.toSet -- assignedReplicas.toSet;
//        assertTrue(String.format("All in sync replicas %s must be in the assigned replica list %s",inSyncReplicas, assignedReplicas),
//                phantomInSyncReplicas.size == 0);
//    }
//
//    public void  ensureNoUnderReplicatedPartitions(ZkClient zkClient, String topic, Integer partitionToBeReassigned, Seq assignedReplicas<Int>,
//                                          Seq servers<KafkaServer>) {
//        TestUtils.waitUntilTrue(() => {
//                val inSyncReplicas = ZkUtils.getInSyncReplicasForPartition(zkClient, topic, partitionToBeReassigned);
//                inSyncReplicas.size == assignedReplicas.size;
//        },
//                String.format("Reassigned partition <%s,%d> is under replicated",topic, partitionToBeReassigned))
//        var Option leader<Int> = None;
//        TestUtils.waitUntilTrue(() => {
//                leader = ZkUtils.getLeaderForPartition(zkClient, topic, partitionToBeReassigned);
//                leader.isDefined;
//        },
//                String.format("Reassigned partition <%s,%d> is unavailable",topic, partitionToBeReassigned))
//        TestUtils.waitUntilTrue(() => {
//                val leaderBroker = servers.filter(s => s.config.brokerId == leader.get).head;
//        leaderBroker.replicaManager.underReplicatedPartitionCount() == 0;
//        },
//        String.format("Reassigned partition <%s,%d> is under-replicated as reported by the leader %d",topic, partitionToBeReassigned, leader.get))
//    }
//
//    public void  checkIfReassignPartitionPathExists(ZkClient zkClient): Boolean = {
//        ZkUtils.pathExists(zkClient, ZkUtils.ReassignPartitionsPath);
//    }
//
//


    public static List<String> sendMessagesToPartition(List<KafkaConfig> configs,
                                                       String topic,
                                                       Integer partition,
                                                       Integer numMessages,
                                                       CompressionCodec compression) {
        if (compression == null) {
            compression = NoCompressionCodec;
        }
        String header = String.format("test-%d", partition);
        Properties props = new Properties();
        props.put("compression.codec", compression.codec.toString());
        Producer<Integer, String> producer =
                createProducer(TestUtils.getBrokerListStrFromConfigs(configs),
                        StringEncoder.class.getName(),
                        IntEncoder.class.getName(),
                        FixedValuePartitioner.class.getName(),
                        props);
        List<String> ms = Sc.itToList(0, numMessages, x -> header + "-" + x);
        producer.send(Sc.map(ms, m -> new KeyedMessage<Integer, String>(topic, partition, m)));
        log.debug(String.format("Sent %d messages for partition <%s,%d>", ms.size(), topic, partition));
        producer.close();
        return ms;
    }

    public static List<String> sendMessages(List<KafkaConfig> configs,
                                            String topic,
                                            String producerId,
                                            Integer messagesPerNode,
                                            String header,
                                            CompressionCodec compression,
                                            Integer numParts) {
        List<String> messages = null;
        Properties props = new Properties();
        props.put("compression.codec", compression.codec.toString());
        props.put("client.id", producerId);
        Producer<Integer, String> producer =
                createProducer(TestUtils.getBrokerListStrFromConfigs(configs),
                        StringEncoder.class.getName(),
                        IntEncoder.class.getName(),
                        FixedValuePartitioner.class.getName(), props);

        for (int partition = 0; partition < numParts; partition++) {
            final int partition_f = partition;
            List<String> ms = Sc.itToList(0, messagesPerNode, x -> header + "-" + partition_f + "-" + x);
            producer.send(Sc.map(ms, m -> new KeyedMessage<Integer, String>(topic, partition_f, m)));
            messages.addAll(ms);
            log.debug(String.format("Sent %d messages for partition <%s,%d>", ms.size(), topic, partition));
        }
        producer.close();
        return messages;
    }


    public static List<String> getMessages(Integer nMessagesPerThread, Map<String, List<KafkaStream<String, String>>> topicMessageStreams) {
        List<String> messages = null;
        topicMessageStreams.forEach((topic, messageStreams) -> {
            for (KafkaStream<String, String> messageStream : messageStreams) {
                ConsumerIterator<String, String> iterator = messageStream.iterator();
                for (int i = 0; i < nMessagesPerThread; i++) {
                    Assert.assertTrue(iterator.hasNext());
                    String message = iterator.next().message();
                    messages.add(message);
                    log.debug("received message: " + message);
                }
            }
        });
        Collections.reverse(messages);
        return messages;
    }

    /**
     * Create new LogManager instance with default configuration for testing
     */
    public static LogManager createLogManager(List<File> logDirs, LogConfig defaultConfig, CleanerConfig cleanerConfig, MockTime time) {
        logDirs = logDirs == null ? Lists.newArrayList() : logDirs;
        defaultConfig = defaultConfig == null ? new LogConfig() : defaultConfig;
        cleanerConfig = cleanerConfig != null ? cleanerConfig : new CleanerConfig(false);
        time = time != null ? time : new MockTime();
        return new LogManager(
                logDirs,
                Maps.newHashMap(),
                defaultConfig,
                cleanerConfig,
                4,
                1000L,
                10000L,
                1000L,
                time.scheduler, new BrokerState(),
                time
        );
    }
}

//
//    object TestZKUtils {
//        val zookeeperConnect = "127.0.0.1:" + TestUtils.choosePort();
//        }
//
class IntEncoder implements Encoder<Integer> {
    public VerifiableProperties props = null;

    public IntEncoder(VerifiableProperties props) {
        this.props = props;
    }

    public byte[] toBytes(Integer n) {
        return n.toString().getBytes();
    }

}

//
//class StaticPartitioner(VerifiableProperties props = null) extends Partitioner{
//        public void  partition(Any data, Integer numPartitions): Integer = {
//        (data.asInstanceOf<String>.length % numPartitions);
//        }
//        }
//
//class HashPartitioner(VerifiableProperties props = null) extends Partitioner {
//        public void  partition(Any data, Integer numPartitions): Integer = {
//        (data.hashCode % numPartitions);
//        }
//        }
//
class FixedValuePartitioner implements Partitioner {
    public VerifiableProperties props = null;

    public FixedValuePartitioner(VerifiableProperties props) {
        this.props = props;
    }

    @Override
    public Integer partition(Object data, Integer numPartitions) {
        return (Integer) data;
    }
}
