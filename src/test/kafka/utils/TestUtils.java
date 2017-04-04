package kafka.utils;

import kafka.message.ByteBufferMessageSet;
import kafka.message.CompressionCodec;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import kafka.utils.IteratorTemplate;
import org.junit.Assert;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Random;

/**
 * Created by Administrator on 2017/3/26.
 */
public class TestUtils {
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

    public static void Assert.assertEquals(String msg, Iterator<MessageAndOffset> expected, Iterator<MessageAndOffset> actual) {
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

        // check if the expected iterator is longer;
        if (expected.hasNext()) {
            int length1 = length;
            while (expected.hasNext()) {
                expected.next();
                length1 += 1;
            }
            Assert.assertFalse("Iterators have uneven length-- first has more: " + length1 + " > " + length, true);
        }

        // check if the actual iterator was longer;
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
                else;
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
//    public void  tempDir(): File = {
//        val f = new File(IoTmpDir, "kafka-" + random.nextInt(1000000));
//        f.mkdirs();
//        f.deleteOnExit();
//
//        Runtime.getRuntime().addShutdownHook(new Thread() {
//            override public void  run() = {
//                Utils.rm(f);
//            }
//        });
//
//        f;
//    }
//
//    /**
//     * Choose a number of random available ports
//     */
//    public void  choosePorts Integer count): List<Int> = {
//        val sockets =
//        for(i <- 0 until count)
//        yield new ServerSocket(0);
//        val socketList = sockets.toList;
//        val ports = socketList.map(_.getLocalPort);
//        socketList.map(_.close);
//        ports;
//    }
//
//    /**
//     * Choose an available port
//     */
//    public void  choosePort(): Integer = choosePorts(1).head;
//
//    public void  tempTopic(): String = "testTopic" + random.nextInt(1000000);
//
//    /**
//     * Create a temporary relative directory
//     */
//    public void  tempRelativeDir(String parent): File = {
//        val f = new File(parent, "kafka-" + random.nextInt(1000000));
//        f.mkdirs();
//        f.deleteOnExit();
//        f;
//    }
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
//    /**
//     * Create a kafka server instance with appropriate test settings
//     * USING THIS IS A SIGN YOU ARE NOT WRITING A REAL UNIT TEST
//     * @param config The configuration of the server
//     */
//    public void  createServer(KafkaConfig config, Time time = SystemTime): KafkaServer = {
//        val server = new KafkaServer(config, time);
//        server.startup();
//        server;
//    }
//
//    /**
//     * Create a test config for the given node id
//     */
//    public void  createBrokerConfigs Integer numConfigs,
//                            Boolean enableControlledShutdown = true): List<Properties> = {
//        for((port, node) <- choosePorts(numConfigs).zipWithIndex)
//        yield createBrokerConfig(node, port, enableControlledShutdown);
//    }
//
//    public void  getBrokerListStrFromConfigs(Seq configs<KafkaConfig>): String = {
//        configs.map(c => formatAddress(c.hostName, c.port)).mkString(",")
//    }
//
//    /**
//     * Create a test config for the given node id
//     */
//    public void  createBrokerConfig Integer nodeId, Integer port = choosePort(),
//    Boolean enableControlledShutdown = true): Properties = {
//        val props = new Properties;
//        props.put("broker.id", nodeId.toString);
//        props.put("host.name", "localhost");
//        props.put("port", port.toString);
//        props.put("log.dir", TestUtils.tempDir().getAbsolutePath);
//        props.put("zookeeper.connect", TestZKUtils.zookeeperConnect);
//        props.put("replica.socket.timeout.ms", "1500");
//        props.put("controlled.shutdown.enable", enableControlledShutdown.toString);
//        props;
//    }
//
//    /**
//     * Create a topic in zookeeper.
//     * Wait until the leader is elected and the metadata is propagated to all brokers.
//     * Return the leader for each partition.
//     */
//    public void  createTopic(ZkClient zkClient,
//                    String topic,
//                    Integer numPartitions = 1,
//                    Integer replicationFactor = 1,
//                    Seq servers<KafkaServer>,
//                    Properties topicConfig = new Properties) : scala.collection.immutable.Map<Int, Option[Int]> = {
//        // create topic;
//        AdminUtils.createTopic(zkClient, topic, numPartitions, replicationFactor, topicConfig);
//        // wait until the update metadata request for new topic reaches all servers;
//        (0 until numPartitions).map { case i =>
//            TestUtils.waitUntilMetadataIsPropagated(servers, topic, i);
//            i -> TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, i);
//        }.toMap;
//    }
//
//    /**
//     * Create a topic in zookeeper using a customized replica assignment.
//     * Wait until the leader is elected and the metadata is propagated to all brokers.
//     * Return the leader for each partition.
//     */
//    public void  createTopic(ZkClient zkClient, String topic, collection partitionReplicaAssignment.Map<Int, Seq[Int]>,
//                    Seq servers<KafkaServer>) : scala.collection.immutable.Map<Int, Option[Int]> = {
//        // create topic;
//        AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, partitionReplicaAssignment);
//        // wait until the update metadata request for new topic reaches all servers;
//        partitionReplicaAssignment.keySet.map { case i =>
//            TestUtils.waitUntilMetadataIsPropagated(servers, topic, i);
//            i -> TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, i);
//        }.toMap;
//    }
//
//    /**
//     * Create a test config for a consumer
//     */
//    public void  createConsumerProperties(String zkConnect, String groupId, String consumerId,
//                                 Long consumerTimeout = -1): Properties = {
//        val props = new Properties;
//        props.put("zookeeper.connect", zkConnect);
//        props.put("group.id", groupId);
//        props.put("consumer.id", consumerId);
//        props.put("consumer.timeout.ms", consumerTimeout.toString);
//        props.put("zookeeper.session.timeout.ms", "6000");
//        props.put("zookeeper.sync.time.ms", "200");
//        props.put("auto.commit.interval.ms", "1000");
//        props.put("rebalance.max.retries", "4");
//        props.put("auto.offset.reset", "smallest");
//        props.put("num.consumer.fetchers", "2");
//
//        props;
//    }
//
//    /**
//     * Wrap the message in a message set
//     * @param payload The bytes of the message
//     */
//    public void  singleMessageSet(Array payload<Byte>, CompressionCodec codec = NoCompressionCodec, Array key<Byte> = null) =
//            new ByteBufferMessageSet(compressionCodec = codec, messages = new Message(payload, key));
//
//    /**
//     * Generate an array of random bytes
//     * @param numBytes The size of the array
//     */
//    public void  randomBytes Integer numBytes): Array<Byte> = {
//        val bytes = new Array<Byte>(numBytes);
//                seededRandom.nextBytes(bytes);
//        bytes;
//    }
//
//    /**
//     * Generate a random string of letters and digits of the given length
//     * @param len The length of the string
//     * @return The random string
//     */
//    public void  randomString Integer len): String = {
//        val b = new StringBuilder();
//        for(i <- 0 until len)
//        b.append(LettersAndDigits.charAt(seededRandom.nextInt(LettersAndDigits.length)));
//        b.toString;
//    }
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
//    /**
//     * Create a producer with a few pre-configured properties.
//     * If certain properties need to be overridden, they can be provided in producerProps.
//     */
//    public void  createProducer<K, V>(String brokerList,
//    String encoder = classOf<DefaultEncoder>.getName,
//    String keyEncoder = classOf<DefaultEncoder>.getName,
//    String partitioner = classOf<DefaultPartitioner>.getName,
//    Properties producerProps = null): Producer<K, V> = {
//        val Properties props = getProducerConfig(brokerList);
//
//        //override any explicitly specified properties;
//        if (producerProps != null)
//            props.putAll(producerProps);
//
//        props.put("serializer.class", encoder);
//        props.put("key.serializer.class", keyEncoder);
//        props.put("partitioner.class", partitioner);
//        new Producer<K, V>(new ProducerConfig(props));
//    }
//
//    /**
//     * Create a (new) producer with a few pre-configured properties.
//     */
//    public void  createNewProducer(String brokerList,
//                          Integer acks = -1,
//                          Long metadataFetchTimeout = 3000L,
//                          Boolean blockOnBufferFull = true,
//                          Long bufferSize = 1024L * 1024L,
//                          Integer retries = 0) : KafkaProducer<Array[Byte],Array[Byte]> = {
//        import org.apache.kafka.clients.producer.ProducerConfig;
//
//        val producerProps = new Properties();
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
//
//    /**
//     * Create a default producer config properties map with the given metadata broker list
//     */
//    public void  getProducerConfig(String brokerList): Properties = {
//        val props = new Properties();
//        props.put("metadata.broker.list", brokerList);
//        props.put("message.send.max.retries", "5");
//        props.put("retry.backoff.ms", "1000");
//        props.put("request.timeout.ms", "2000");
//        props.put("request.required.acks", "-1");
//        props.put("send.buffer.bytes", "65536");
//        props.put("connect.timeout.ms", "100000");
//        props.put("reconnect.interval", "10000");
//
//        props;
//    }
//
//    public void  getSyncProducerConfig Integer port): Properties = {
//        val props = new Properties();
//        props.put("host", "localhost");
//        props.put("port", port.toString);
//        props.put("request.timeout.ms", "500");
//        props.put("request.required.acks", "1");
//        props.put("serializer.class", classOf<StringEncoder>.getName);
//        props;
//    }
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
//    /**
//     * Create a wired format request based on simple basic information
//     */
//    public void  produceRequest(String topic,
//                       Integer partition,
//                       ByteBufferMessageSet message,
//                       Integer acks = SyncProducerConfig.DefaultRequiredAcks,
//                       Integer timeout = SyncProducerConfig.DefaultAckTimeoutMs,
//                       Integer correlationId = 0,
//                       String clientId = SyncProducerConfig.DefaultClientId): ProducerRequest = {
//        produceRequestWithAcks(Seq(topic), Seq(partition), message, acks, timeout, correlationId, clientId);
//    }
//
//    public void  produceRequestWithAcks(Seq topics<String>,
//                               Seq partitions<Int>,
//                               ByteBufferMessageSet message,
//                               Integer acks = SyncProducerConfig.DefaultRequiredAcks,
//                               Integer timeout = SyncProducerConfig.DefaultAckTimeoutMs,
//                               Integer correlationId = 0,
//                               String clientId = SyncProducerConfig.DefaultClientId): ProducerRequest = {
//        val data = topics.flatMap(topic =>
//        partitions.map(partition => (TopicAndPartition(topic,  partition), message));
//        );
//        new ProducerRequest(correlationId, clientId, acks.toShort, timeout, collection.mutable.Map(_ data*));
//    }
//
//    public void  makeLeaderForPartition(ZkClient zkClient, String topic,
//                               scala leaderPerPartitionMap.collection.immutable.Map<Int, Int>,
//                               Integer controllerEpoch) {
//        leaderPerPartitionMap.foreach;
//        {
//            leaderForPartition => {
//            val partition = leaderForPartition._1;
//            val leader = leaderForPartition._2;
//            try{
//                val currentLeaderAndIsrOpt = ZkUtils.getLeaderAndIsrForPartition(zkClient, topic, partition);
//                var LeaderAndIsr newLeaderAndIsr = null;
//                if(currentLeaderAndIsrOpt == None)
//                    newLeaderAndIsr = new LeaderAndIsr(leader, List(leader));
//                else{
//                    newLeaderAndIsr = currentLeaderAndIsrOpt.get;
//                    newLeaderAndIsr.leader = leader;
//                    newLeaderAndIsr.leaderEpoch += 1;
//                    newLeaderAndIsr.zkVersion += 1;
//                }
//                ZkUtils.updatePersistentPath(zkClient, ZkUtils.getTopicPartitionLeaderAndIsrPath(topic, partition),
//                        ZkUtils.leaderAndIsrZkData(newLeaderAndIsr, controllerEpoch));
//            } catch {
//                case Throwable oe => error(String.format("Error while electing leader for partition <%s,%d>",topic, partition), oe)
//            }
//        }
//        }
//    }
//
//    /**
//     *  If neither oldLeaderOpt nor newLeaderOpt is defined, wait until the leader of a partition is elected.
//     *  If oldLeaderOpt is defined, it waits until the new leader is different from the old leader.
//     *  If newLeaderOpt is defined, it waits until the new leader becomes the expected new leader.
//     * @return The new leader or assertion failure if timeout is reached.
//     */
//    public void  waitUntilLeaderIsElectedOrChanged(ZkClient zkClient, String topic, Integer partition, Long timeoutMs = 5000L,
//                                          Option oldLeaderOpt<Int> = None, Option newLeaderOpt<Int> = None): Option<Int> = {
//        require(!(oldLeaderOpt.isDefined && newLeaderOpt.isDefined), "Can't define both the old and the new leader");
//        val startTime = System.currentTimeMillis();
//        var isLeaderElectedOrChanged = false;
//
//        trace("Waiting for leader to be elected or changed for partition <%s,%d>, older leader is %s, new leader is %s";
//                .format(topic, partition, oldLeaderOpt, newLeaderOpt))
//
//        var Option leader<Int> = None;
//        while (!isLeaderElectedOrChanged && System.currentTimeMillis() < startTime + timeoutMs) {
//            // check if leader is elected;
//            leader = ZkUtils.getLeaderForPartition(zkClient, topic, partition);
//            leader match {
//                case Some(l) =>
//                    if (newLeaderOpt.isDefined && newLeaderOpt.get == l) {
//                        trace(String.format("Expected new leader %d is elected for partition <%s,%d>",l, topic, partition))
//                        isLeaderElectedOrChanged = true;
//                    } else if (oldLeaderOpt.isDefined && oldLeaderOpt.get != l) {
//                        trace(String.format("Leader for partition <%s,%d> is changed from %d to %d",topic, partition, oldLeaderOpt.get, l))
//                        isLeaderElectedOrChanged = true;
//                    } else if (!oldLeaderOpt.isDefined) {
//                        trace(String.format("Leader %d is elected for partition <%s,%d>",l, topic, partition))
//                        isLeaderElectedOrChanged = true;
//                    } else {
//                        trace(String.format("Current leader for partition <%s,%d> is %d",topic, partition, l))
//                    }
//                case None =>
//                    trace(String.format("Leader for partition <%s,%d> is not elected yet",topic, partition))
//            }
//            Thread.sleep(timeoutMs.min(100L));
//        }
//        if (!isLeaderElectedOrChanged)
//            fail("Timing out after %d ms since leader is not elected or changed for partition <%s,%d>";
//                    .format(timeoutMs, topic, partition))
//
//        return leader;
//    }
//
//    /**
//     * Execute the given block. If it throws an assert error, retry. Repeat
//     * until no error is thrown or the time limit ellapses
//     */
//    public void  retry(Long maxWaitMs)(block: => Unit) {
//        var wait = 1L;
//        val startTime = System.currentTimeMillis();
//        while(true) {
//            try {
//                block;
//                return;
//            } catch {
//                case AssertionFailedError e =>
//                    val ellapsed = System.currentTimeMillis - startTime;
//                    if(ellapsed > maxWaitMs) {
//                        throw e;
//                    } else {
//                        info("Attempt failed, sleeping for " + wait + ", and then retrying.")
//                        Thread.sleep(wait);
//                        wait += math.min(wait, 1000);
//                    }
//            }
//        }
//    }
//
//    /**
//     * Wait until the given condition is true or throw an exception if the given wait time elapses.
//     */
//    public void  waitUntilTrue(condition: () => Boolean, String msg, Long waitTime = 5000L): Boolean = {
//        val startTime = System.currentTimeMillis();
//        while (true) {
//            if (condition())
//                return true;
//            if (System.currentTimeMillis() > startTime + waitTime)
//                fail(msg);
//            Thread.sleep(waitTime.min(100L));
//        }
//        // should never hit here;
//        throw new RuntimeException("unexpected error");
//    }
//
//    public void  isLeaderLocalOnBroker(String topic, Integer partitionId, KafkaServer server): Boolean = {
//        val partitionOpt = server.replicaManager.getPartition(topic, partitionId);
//        partitionOpt match {
//            case None => false;
//            case Some(partition) =>
//                val replicaOpt = partition.leaderReplicaIfLocal;
//                replicaOpt match {
//                case None => false;
//                case Some(_) => true;
//            }
//        }
//    }
//
//    public void  createRequestByteBuffer(RequestOrResponse request): ByteBuffer = {
//        val byteBuffer = ByteBuffer.allocate(request.sizeInBytes + 2);
//        byteBuffer.putShort(request.requestId.get);
//        request.writeTo(byteBuffer);
//        byteBuffer.rewind();
//        byteBuffer;
//    }
//
//
//    /**
//     * Wait until a valid leader is propagated to the metadata cache in each broker.
//     * It assumes that the leader propagated to each broker is the same.
//     * @param servers The list of servers that the metadata should reach to
//     * @param topic The topic name
//     * @param partition The partition Id
//     * @param timeout The amount of time waiting on this condition before assert to fail
//     * @return The leader of the partition.
//     */
//    public void  waitUntilMetadataIsPropagated(Seq servers<KafkaServer>, String topic, Integer partition, Long timeout = 5000L): Integer = {
//        var Integer leader = -1;
//        TestUtils.waitUntilTrue(() =>
//                servers.foldLeft(true) {
//            (result, server) =>
//            val partitionStateOpt = server.apis.metadataCache.getPartitionInfo(topic, partition);
//            partitionStateOpt match {
//                case None => false;
//                case Some(partitionState) =>
//                    leader = partitionState.leaderIsrAndControllerEpoch.leaderAndIsr.leader;
//                    result && Request.isValidBrokerId(leader);
//            }
//        },
//        String.format("Partition <%s,%d> metadata not propagated after %d ms",topic, partition, timeout),
//                waitTime = timeout);
//
//        leader;
//    }
//
//    public void  writeNonsenseToFile(File fileName, Long position, Integer size) {
//        val file = new RandomAccessFile(fileName, "rw");
//        file.seek(position);
//        for(i <- 0 until size)
//        file.writeByte(random.nextInt(255));
//        file.close();
//    }
//
//    public void  appendNonsenseToFile(File fileName, Integer size) {
//        val file = new FileOutputStream(fileName, true);
//        for(i <- 0 until size)
//        file.write(random.nextInt(255));
//        file.close();
//    }
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
//    /**
//     * Create new LogManager instance with default configuration for testing
//     */
//    public void  createLogManager(
//            Array logDirs<File> = Array.empty<File>,
//            LogConfig defaultConfig = LogConfig(),
//    CleanerConfig cleanerConfig = CleanerConfig(enableCleaner = false),
//    MockTime time = new MockTime()) =
//    {
//        new LogManager(
//                logDirs = logDirs,
//                topicConfigs = Map(),
//                defaultConfig = defaultConfig,
//                cleanerConfig = cleanerConfig,
//                ioThreads = 4,
//                flushCheckMs = 1000L,
//                flushCheckpointMs = 10000L,
//                retentionCheckMs = 1000L,
//                scheduler = time.scheduler,
//                time = time,
//                brokerState = new BrokerState());
//    }
//
//    public void  sendMessagesToPartition(Seq configs<KafkaConfig>,
//                                String topic,
//                                Integer partition,
//                                Integer numMessages,
//                                CompressionCodec compression = NoCompressionCodec): List<String> = {
//        val header = String.format("test-%d",partition)
//        val props = new Properties();
//        props.put("compression.codec", compression.codec.toString);
//        val Producer producer<Int, String> =
//        createProducer(TestUtils.getBrokerListStrFromConfigs(configs),
//                encoder = classOf<StringEncoder>.getName,
//                keyEncoder = classOf<IntEncoder>.getName,
//                partitioner = classOf<FixedValuePartitioner>.getName,
//                producerProps = props);
//
//        val ms = 0.until(numMessages).map(x => header + "-" + x);
//        producer.send(ms.map(m => new KeyedMessage<Int, String>(topic, partition, m)):_*);
//        debug(String.format("Sent %d messages for partition <%s,%d>",ms.size, topic, partition))
//        producer.close();
//        ms.toList;
//    }
//
//    public void  sendMessages(Seq configs<KafkaConfig>,
//                     String topic,
//                     String producerId,
//                     Integer messagesPerNode,
//                     String header,
//                     CompressionCodec compression,
//                     Integer numParts): List<String>= {
//        var List messages<String> = Nil;
//        val props = new Properties();
//        props.put("compression.codec", compression.codec.toString);
//        props.put("client.id", producerId);
//        val   Producer producer<Int, String> =
//        createProducer(brokerList = TestUtils.getBrokerListStrFromConfigs(configs),
//                encoder = classOf<StringEncoder>.getName,
//                keyEncoder = classOf<IntEncoder>.getName,
//                partitioner = classOf<FixedValuePartitioner>.getName,
//                producerProps = props);
//
//        for (partition <- 0 until numParts) {
//            val ms = 0.until(messagesPerNode).map(x => header + "-" + partition + "-" + x);
//            producer.send(ms.map(m => new KeyedMessage<Int, String>(topic, partition, m)):_*);
//            messages ++= ms;
//            debug(String.format("Sent %d messages for partition <%s,%d>",ms.size, topic, partition))
//        }
//        producer.close();
//        messages;
//    }
//
//    public void  getMessages Integer nMessagesPerThread,
//                    Map topicMessageStreams[String, List<KafkaStream[String, String]]>): List<String> = {
//        var List messages<String> = Nil;
//        for ((topic, messageStreams) <- topicMessageStreams) {
//            for (messageStream <- messageStreams) {
//                val iterator = messageStream.iterator;
//                for (i <- 0 until nMessagesPerThread) {
//                    assertTrue(iterator.hasNext);
//                    val message = iterator.next.message;
//                    messages ::= message;
//                    debug("received message: " + message);
//                }
//            }
//        }
//        messages.reverse;
//    }
//}
//
//    object TestZKUtils {
//        val zookeeperConnect = "127.0.0.1:" + TestUtils.choosePort();
//        }
//
//class IntEncoder(VerifiableProperties props = null) extends Encoder<Int> {
//        override public void  toBytes Integer n) = n.toString.getBytes;
//        }
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
//class FixedValuePartitioner(VerifiableProperties props = null) extends Partitioner {
//        public void  partition(Any data, Integer numPartitions): Integer = data.asInstanceOf<Int>;
//        }
}
