package kafka.server;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import kafka.admin.AdminUtils;
import kafka.api.*;
import kafka.common.ErrorMapping;
import kafka.consumer.SimpleConsumer;
import kafka.log.Log;
import kafka.log.LogManager;
import kafka.log.TopicAndPartition;
import kafka.message.ByteBufferMessageSet;
import kafka.message.CompressionCodec;
import kafka.message.Message;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.Time;
import kafka.zk.ZooKeeperTestHarness;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import static kafka.utils.TestUtils.*;
import static org.junit.Assert.assertFalse;

public class LogOffsetTest extends ZooKeeperTestHarness {
    Random random = new Random();
    File logDir = null;
    File topicLogDir = null;
    KafkaServer server = null;
    Integer logSize = 100;
    Integer brokerPort = 9099;
    SimpleConsumer simpleConsumer = null;
    Time time = new MockTime();

    @Before
    @Override
    public void setUp() throws IOException, InterruptedException {
        super.setUp();
        Properties config = createBrokerConfig(1, brokerPort);
        String logDirPath = config.getProperty("log.dir");
        logDir = new File(logDirPath);
        time = new MockTime();
        server = TestUtils.createServer(new KafkaConfig(config), time);
        simpleConsumer = new SimpleConsumer("localhost", brokerPort, 1000000, 64 * 1024, "");
    }

//    @After
//    @Override
//    public void tearDown() {
//        simpleConsumer.close();
//        server.shutdown();
//        Utils.rm(logDir);
//        super.tearDown();
//    }

    @Test
    public void testGetOffsetsForUnknownTopic() {
        TopicAndPartition topicAndPartition = new TopicAndPartition("foo", 0);
        OffsetRequest request = new OffsetRequest(
                ImmutableMap.of(topicAndPartition, new PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 10)), OffsetRequest.DefaultClientId, Request.OrdinaryConsumerId);
        OffsetResponse offsetResponse = simpleConsumer.getOffsetsBefore(request);
        Assert.assertEquals(ErrorMapping.UnknownTopicOrPartitionCode,
                offsetResponse.partitionErrorAndOffsets.get(topicAndPartition).error);
    }

    @Test
    public void testGetOffsetsBeforeLatestTime() {
        String topicPartition = "kafka-" + 0;
        String topic = topicPartition.split("-")[0];
        Integer part = Integer.valueOf(topicPartition.split("-")[1]).intValue();

        // setup brokers in zookeeper as owners of partitions for this test;
        AdminUtils.createTopic(zkClient, topic, 1, 1);

        LogManager logManager = server.getLogManager();
        TestUtils.waitUntilTrue(() -> logManager.getLog(new TopicAndPartition(topic, part)).isPresent(),
                "Log for partition <topic,0> should be created");
        Log log = logManager.getLog(new TopicAndPartition(topic, part)).get();

        Message message = new Message(Integer.toString(42).getBytes());
        for (int i = 0; i < 20; i++)
            log.append(new ByteBufferMessageSet(CompressionCodec.NoCompressionCodec, message));
        log.flush();

        List<Long> offsets = server.apis.fetchOffsets(logManager, new TopicAndPartition(topic, part), OffsetRequest.LatestTime, 10);
        TestUtils.assertEquals(Lists.newArrayList(20L, 18L, 15L, 12L, 9L, 6L, 3L, 0L), offsets);

        waitUntilTrue(() -> isLeaderLocalOnBroker(topic, part, server), "Leader should be elected");
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, part);
        OffsetRequest offsetRequest = new OffsetRequest(ImmutableMap.of(topicAndPartition, new PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 10)), OffsetRequest.DefaultClientId, 0);
        List<Long> consumerOffsets = simpleConsumer.getOffsetsBefore(offsetRequest).partitionErrorAndOffsets.get(topicAndPartition).offsets;
        TestUtils.assertEquals(Lists.newArrayList(20L, 18L, 15L, 12L, 9L, 6L, 3L, 0L), consumerOffsets);

        // try to fetch using latest offset;
        FetchResponse fetchResponse = simpleConsumer.fetch(new FetchRequestBuilder().addFetch(topic, 0, consumerOffsets.get(0), 300 * 1024).build());
        Assert.assertFalse(fetchResponse.messageSet(topic, 0).iterator().hasNext());
    }

    @Test
    public void testEmptyLogsGetOffsets() {
        String topicPartition = "kafka-" + random.nextInt(10);
        String topicPartitionPath = getLogDir().getAbsolutePath() + "/" + topicPartition;
        topicLogDir = new File(topicPartitionPath);
        topicLogDir.mkdir();

        String topic = topicPartition.split("-")[0];

        // setup brokers in zookeeper as owners of partitions for this test;
        createTopic(zkClient, topic, 1, 1, Lists.newArrayList(server), null);

        boolean offsetChanged = false;
        for (int i = -1; i < 14; i++) {
            TopicAndPartition topicAndPartition = new TopicAndPartition(topic, 0);
            OffsetRequest offsetRequest = new OffsetRequest(ImmutableMap.of(topicAndPartition, new PartitionOffsetRequestInfo(OffsetRequest.EarliestTime, 1)));
            List<Long> consumerOffsets = simpleConsumer.getOffsetsBefore(offsetRequest).partitionErrorAndOffsets.get(topicAndPartition).offsets;

            if (consumerOffsets.get(0) == 1) {
                offsetChanged = true;
            }
        }
        assertFalse(offsetChanged);
    }

    @Test
    public void testGetOffsetsBeforeNow() {
        String topicPartition = "kafka-" + random.nextInt(3);
        String topic = topicPartition.split("-")[0];
        int part = Integer.valueOf(topicPartition.split("-")[1]).intValue();

        // setup brokers in zookeeper as owners of partitions for this test;
        AdminUtils.createTopic(zkClient, topic, 3, 1);

        LogManager logManager = server.getLogManager();
        Log log = logManager.createLog(new TopicAndPartition(topic, part), logManager.defaultConfig);
        Message message = new Message(Integer.toString(42).getBytes());
        for (int i = 0; i < 20; i++)
            log.append(new ByteBufferMessageSet(CompressionCodec.NoCompressionCodec, message));
        log.flush();

        long now = time.milliseconds() + 30000; // pretend it is the future to avoid race conditions with the fs;

        List<Long> offsets = server.apis.fetchOffsets(logManager, new TopicAndPartition(topic, part), now, 10);
        TestUtils.assertEquals(Lists.newArrayList(20L, 18L, 15L, 12L, 9L, 6L, 3L, 0L), offsets);

        waitUntilTrue(() -> isLeaderLocalOnBroker(topic, part, server), "Leader should be elected");
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, part);
        OffsetRequest offsetRequest = new OffsetRequest(ImmutableMap.of(topicAndPartition, new PartitionOffsetRequestInfo(now, 10)), OffsetRequest.DefaultClientId, 0);
        List<Long> consumerOffsets = simpleConsumer.getOffsetsBefore(offsetRequest).partitionErrorAndOffsets.get(topicAndPartition).offsets;
        TestUtils.assertEquals(Lists.newArrayList(20L, 18L, 15L, 12L, 9L, 6L, 3L, 0L), consumerOffsets);
    }

    @Test
    public void testGetOffsetsBeforeEarliestTime() {
        String topicPartition = "kafka-" + random.nextInt(3);
        String topic = topicPartition.split("-")[0];
        int part = Integer.valueOf(topicPartition.split("-")[1]).intValue();

        // setup brokers in zookeeper as owners of partitions for this test;
        AdminUtils.createTopic(zkClient, topic, 3, 1);

        LogManager logManager = server.getLogManager();
        Log log = logManager.createLog(new TopicAndPartition(topic, part), logManager.defaultConfig);
        Message message = new Message(Integer.toString(42).getBytes());
        for (int i = 0; i < 20; i++)
            log.append(new ByteBufferMessageSet(CompressionCodec.NoCompressionCodec, message));
        log.flush();

        List<Long> offsets = server.apis.fetchOffsets(logManager, new TopicAndPartition(topic, part), OffsetRequest.EarliestTime, 10);

        Assert.assertEquals(Lists.newArrayList(0L), offsets);

        waitUntilTrue(() -> isLeaderLocalOnBroker(topic, part, server), "Leader should be elected");
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, part);
        OffsetRequest offsetRequest =
                new OffsetRequest(ImmutableMap.of(topicAndPartition, new PartitionOffsetRequestInfo(OffsetRequest.EarliestTime, 10)));
        List<Long> consumerOffsets =
                simpleConsumer.getOffsetsBefore(offsetRequest).partitionErrorAndOffsets.get(topicAndPartition).offsets;
        Assert.assertEquals(Lists.newArrayList(0L), consumerOffsets);
    }

    private Properties createBrokerConfig(Integer nodeId, Integer port) {
        Properties props = new Properties();
        props.put("broker.id", nodeId.toString());
        props.put("port", port.toString());
        props.put("log.dir", getLogDir().getAbsolutePath());
        props.put("log.flush.interval.messages", "1");
        props.put("enable.zookeeper", "false");
        props.put("num.partitions", "20");
        props.put("log.retention.hours", "10");
        props.put("log.retention.check.interval.ms", (5 * 1000 * 60) + "");
        props.put("log.segment.bytes", logSize.toString());
        props.put("zookeeper.connect", zkConnect.toString());
        return props;
    }

    private File getLogDir() {
        File dir = TestUtils.tempDir();
        return dir;
    }

}
