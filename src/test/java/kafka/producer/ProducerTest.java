package kafka.producer;

import com.google.common.collect.Lists;
import kafka.api.FetchRequestBuilder;
import kafka.api.FetchResponse;
import kafka.common.FailedToSendMessageException;
import kafka.consumer.SimpleConsumer;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import kafka.message.MessageSet;
import kafka.serializer.DefaultEncoder;
import kafka.serializer.StringEncoder;
import kafka.server.KafkaConfig;
import kafka.server.KafkaRequestHandler;
import kafka.server.KafkaServer;
import kafka.utils.*;
import kafka.zk.ZooKeeperTestHarness;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

/**
 * @author zhoulf
 * @create 2017-11-27 14 15
 **/

public class ProducerTest extends ZooKeeperTestHarness {
    private static final Logging log = Logging.getLogger(ProducerTest.class.getName());
    private int brokerId1 = 0;
    private int brokerId2 = 1;
    private List<Integer> ports = TestUtils.choosePorts(2);
    private Integer port1 = ports.get(0);
    private Integer port2 = ports.get(1);
    private KafkaServer server1 = null;
    private KafkaServer server2 = null;
    private SimpleConsumer consumer1 = null;
    private SimpleConsumer consumer2 = null;
    private Logger requestHandlerLogger = Logger.getLogger(KafkaRequestHandler.class);
    private List<KafkaServer> servers = Lists.newArrayList();
    private Properties props1 = TestUtils.createBrokerConfig(brokerId1, port1, false);
    private KafkaConfig config1;
    private Properties props2 = TestUtils.createBrokerConfig(brokerId2, port2, false);
    private KafkaConfig config2;

    @Before
    public void setUp() throws IOException, InterruptedException {
        super.setUp();
        props2.put("num.partitions", "4");
        props1.put("num.partitions", "4");
        config1 = new KafkaConfig(props1);
        config2 = new KafkaConfig(props2);
        // set up 2 brokers with 4 partitions each;
        server1 = TestUtils.createServer(config1);
        server2 = TestUtils.createServer(config2);
        servers = Lists.newArrayList(server1, server2);

        Properties props = new Properties();
        props.put("host", "localhost");
        props.put("port", port1.toString());

        consumer1 = new SimpleConsumer("localhost", port1, 1000000, 64 * 1024, "");
        consumer2 = new SimpleConsumer("localhost", port2, 100, 64 * 1024, "");

        // temporarily set request handler logger to a higher level;
        requestHandlerLogger.setLevel(Level.FATAL);
    }

    @Override
    public void tearDown() {
        // restore set request handler logger to a higher level;
        requestHandlerLogger.setLevel(Level.ERROR);

        if (consumer1 != null)
            consumer1.close();
        if (consumer2 != null)
            consumer2.close();

        server1.shutdown();
        server2.shutdown();
        Utils.rm(server1.config.logDirs);
        Utils.rm(server2.config.logDirs);
        super.tearDown();
    }

    @Test
    public void testUpdateBrokerPartitionInfo() {
        String topic = "new-topic";
        TestUtils.createTopic(zkClient, topic, 1, 2, servers, null);

        Properties props = new Properties();
        // no need to retry since the send will always fail;
        props.put("message.send.max.retries", "0");
//        Producer<String, String> producer1 = TestUtils.createProducer(
//                "localhost:80,localhost:81",
//                StringEncoder.class.getName(),
//                StringEncoder.class.getName(),
//                DefaultPartitioner.class.getName(),
//                props);

//        try {
//            producer1.send(new KeyedMessage(topic, "test", "test1"));
//            Assert.fail("Test should fail because the broker list provided are not valid");
//        } catch (FailedToSendMessageException e) { // this is expected;
//        } catch (Throwable oe) {
//            Assert.fail("Should succeed sending the message");
//            oe.printStackTrace();
//        } finally {
//            producer1.close();
//        }
//
        Producer<String, String> producer2 = TestUtils.createProducer(
                 TestUtils.getBrokerListStrFromConfigs(Lists.newArrayList(config1)),
                StringEncoder.class.getName(),
                StringEncoder.class.getName(),
                DefaultPartitioner.class.getName()
                , null);

        try {
            producer2.send(new KeyedMessage<>(topic, "test", "test1"));
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail("Should succeed sending the message");
        } finally {
            producer2.close();
        }
//
//        Producer<String, String> producer3 = TestUtils.createProducer(
//                TestUtils.getBrokerListStrFromConfigs(Lists.newArrayList(config1, config2)),
//                StringEncoder.class.getName(),
//                StringEncoder.class.getName(),
//                DefaultPartitioner.class.getName(),
//                null);
//
//        try {
//            producer3.send(new KeyedMessage<>(topic, "test", "test1"));
//        } catch (Throwable e) {
//            Assert.fail("Should succeed sending the message");
//            e.printStackTrace();
//        } finally {
//            producer3.close();
//        }
    }

    @Test
    public void testSendToNewTopic() {
        Properties props1 = new Properties();
        props1.put("request.required.acks", "-1");

        String topic = "new-topic";
        // create topic with 1 partition and await leadership;
        TestUtils.createTopic(zkClient, topic, 1, 2, servers, null);

        Producer<String, String> producer1 = TestUtils.createProducer(
                TestUtils.getBrokerListStrFromConfigs(Lists.newArrayList(config1, config2)),
                StringEncoder.class.getName(),
                StringEncoder.class.getName(),
                StaticPartitioner.class.getName(),
                props1);

        // Available partition ids should be 0.;
        producer1.send(new KeyedMessage<>(topic, "test", "test1"));
        producer1.send(new KeyedMessage<>(topic, "test", "test2"));
        // get the leader;
        Optional<Integer> leaderOpt = ZkUtils.getLeaderForPartition(zkClient, topic, 0);
        Assert.assertTrue("Leader for topic new-topic partition 0 should exist", leaderOpt.isPresent());
        Integer leader = leaderOpt.get();

        List<MessageAndOffset> messageSet ;
        if (leader == server1.config.brokerId) {
            FetchResponse response1 = consumer1.fetch(new FetchRequestBuilder().addFetch(topic, 0, 0L, 10000).build());
            messageSet = response1.messageSet("new-topic", 0).toMessageAndOffsetList();
        } else {
            FetchResponse response2 = consumer2.fetch(new FetchRequestBuilder().addFetch(topic, 0, 0L, 10000).build());
            messageSet = response2.messageSet("new-topic", 0).toMessageAndOffsetList();
        }
        Assert.assertEquals("Should have fetched 2 messages", 2, messageSet.size());
        Assert.assertEquals(new Message("test1".getBytes(), "test".getBytes()), messageSet.get(0).message);
        Assert.assertEquals(new Message("test2".getBytes(), "test".getBytes()), messageSet.get(1).message);
        producer1.close();

        Properties props2 = new Properties();
        props2.put("request.required.acks", "3");
        // no need to retry since the send will always fail;
        props2.put("message.send.max.retries", "0");

        try {
            Producer<String, String> producer2 = TestUtils.createProducer(
                    TestUtils.getBrokerListStrFromConfigs(Lists.newArrayList(config1, config2)),
                    StringEncoder.class.getName(),
                    StringEncoder.class.getName(),
                    StaticPartitioner.class.getName(),
                    props2);
            producer2.close();
            Assert.fail("we don't support request.required.acks greater than 1");
        } catch (IllegalArgumentException iae) {
            // this is expected;
        } catch (Throwable e) {
            Assert.fail("Not expected :" + e.getMessage());
        }
    }
//
//
//    @Test
//    public void testSendWithDeadBroker() {
//        val props = new Properties();
//        props.put("request.required.acks", "1");
//        // No need to retry since the topic will be created beforehand and normal send will succeed on the first try.;
//        // Reducing the retries will save the time on the subsequent failure test.;
//        props.put("message.send.max.retries", "0");
//
//        val topic = "new-topic";
//        // create topic;
//        TestUtils.createTopic(zkClient, topic, partitionReplicaAssignment = Map(0->Seq(0), 1->Seq(0), 2->Seq(0), 3->
//        Seq(0)),
//        servers = servers);
//
//        val producer = TestUtils.createProducer < String, String>(
//                brokerList = TestUtils.getBrokerListStrFromConfigs(Seq(config1, config2)),
//                encoder = classOf < StringEncoder >.getName,
//                keyEncoder = classOf < StringEncoder >.getName,
//                partitioner = classOf < StaticPartitioner >.getName,
//                producerProps = props);
//
//        try {
//            // Available partition ids should be 0, 1, 2 and 3, all lead and hosted only;
//            // on broker 0;
//            producer.send(new KeyedMessage<String, String>(topic, "test", "test1"));
//        } catch {
//            case Throwable e -> fail("Unexpected exception: " + e);
//        }
//
//        // kill the broker;
//        server1.shutdown;
//        server1.awaitShutdown();
//
//        try {
//            // These sends should fail since there are no available brokers;
//            producer.send(new KeyedMessage<String, String>(topic, "test", "test1"));
//            fail("Should fail since no leader exists for the partition.")
//        } catch {
//            case e:
//                TestFailedException -> throw e // catch and re-throw the failure message;
//            case Throwable e2 -> // otherwise success;
//        }
//
//        // restart server 1;
//        server1.startup();
//        TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, 0);
//        TestUtils.waitUntilMetadataIsPropagated(servers, topic, 0);
//
//        try {
//            // cross check if broker 1 got the messages;
//            val response1 = consumer1.fetch(new FetchRequestBuilder().addFetch(topic, 0, 0, 10000).build());
//            val messageSet1 = response1.messageSet(topic, 0).iterator;
//            Assert.assertTrue("Message set should have 1 message", messageSet1.hasNext);
//            Assert.assertEquals(new Message(bytes = "test1".getBytes, key = "test".getBytes), messageSet1.next.message);
//            assertFalse("Message set should have another message", messageSet1.hasNext);
//        } catch {
//            case Exception e -> fail("Not expected", e);
//        }
//        producer.close;
//    }
//
//    @Test
//    public void testAsyncSendCanCorrectlyFailWithTimeout() {
//        val timeoutMs = 500;
//        val props = new Properties();
//        props.put("request.timeout.ms", String.valueOf(timeoutMs));
//        props.put("request.required.acks", "1");
//        props.put("message.send.max.retries", "0");
//        props.put("client.id", "ProducerTest-testAsyncSendCanCorrectlyFailWithTimeout");
//        val producer = TestUtils.createProducer < String, String>(
//                brokerList = TestUtils.getBrokerListStrFromConfigs(Seq(config1, config2)),
//                encoder = classOf < StringEncoder >.getName,
//                keyEncoder = classOf < StringEncoder >.getName,
//                partitioner = classOf < StaticPartitioner >.getName,
//                producerProps = props);
//
//        val topic = "new-topic";
//        // create topics in ZK;
//        TestUtils.createTopic(zkClient, topic, partitionReplicaAssignment = Map(0->Seq(0, 1)),servers = servers);
//
//        // do a simple test to make sure plumbing is okay;
//        try {
//            // this message should be assigned to partition 0 whose leader is on broker 0;
//            producer.send(new KeyedMessage<String, String>(topic, "test", "test"));
//            // cross check if brokers got the messages;
//            val response1 = consumer1.fetch(new FetchRequestBuilder().addFetch(topic, 0, 0, 10000).build());
//            val messageSet1 = response1.messageSet("new-topic", 0).iterator;
//            Assert.assertTrue("Message set should have 1 message", messageSet1.hasNext);
//            Assert.assertEquals(new Message("test".getBytes), messageSet1.next.message);
//        } catch {
//            case Throwable e -> case Exception e -> producer.close;
//                fail("Not expected", e);
//        }
//
//        // stop IO threads and request handling, but leave networking operational;
//        // any requests should be accepted and queue up, but not handled;
//        server1.requestHandlerPool.shutdown();
//
//        val t1 = SystemTime.milliseconds;
//        try {
//            // this message should be assigned to partition 0 whose leader is on broker 0, but;
//            // broker 0 will not response within timeoutMs millis.;
//            producer.send(new KeyedMessage<String, String>(topic, "test", "test"));
//        } catch {
//            case FailedToSendMessageException e -> /* success */
//            case Exception e -> fail("Not expected", e);
//        } finally{
//            producer.close();
//        }
//        val t2 = SystemTime.milliseconds;
//
//        // make sure we don't wait fewer than timeoutMs;
//        Assert.assertTrue((t2 - t1) >= timeoutMs);
//    }
//
//    @Test
//    public void testSendNullMessage() {
//        val producer = TestUtils.createProducer < String, String>(
//                brokerList = TestUtils.getBrokerListStrFromConfigs(Seq(config1, config2)),
//                encoder = classOf < StringEncoder >.getName,
//                keyEncoder = classOf < StringEncoder >.getName,
//                partitioner = classOf < StaticPartitioner >.getName);
//
//        try {
//
//            // create topic;
//            AdminUtils.createTopic(zkClient, "new-topic", 2, 1);
//            TestUtils.waitUntilTrue(() ->
//                            AdminUtils.fetchTopicMetadataFromZk("new-topic", zkClient).errorCode != ErrorMapping.UnknownTopicOrPartitionCode,
//                    "Topic new-topic not created after timeout",
//                    waitTime = zookeeper.tickTime);
//            TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, "new-topic", 0);
//
//            producer.send(new KeyedMessage<String, String>("new-topic", "key", null));
//        } finally {
//            producer.close();
//        }
//    }
}

