package kafka.server;

import com.google.common.collect.Lists;
import kafka.api.*;
import kafka.common.ErrorMapping;
import kafka.common.OffsetAndMetadata;
import kafka.common.OffsetMetadataAndError;
import kafka.consumer.SimpleConsumer;
import kafka.log.TopicAndPartition;
import kafka.utils.MockTime;
import kafka.utils.Sc;
import kafka.utils.TestUtils;
import kafka.utils.Time;
import kafka.zk.ZooKeeperTestHarness;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import static kafka.utils.TestUtils.*;

public class OffsetCommitTest extends ZooKeeperTestHarness {
    Random random = new Random();
    File logDir = null;
    File topicLogDir = null;
    KafkaServer server = null;
    Integer logSize = 100;
    Integer brokerPort = 9099;
    String group = "test-group";
    SimpleConsumer simpleConsumer = null;
    Time time = new MockTime();

    @Before
    @Override
    public void setUp() throws IOException, InterruptedException {
        super.setUp();
        Properties config = createBrokerConfig(1, brokerPort, null);
        // Currently TODO, when there is no topic in a cluster, the controller doesn't send any UpdateMetadataRequest to;
        // the broker. As a result, the live broker list in metadataCache is empty. This causes the ConsumerMetadataRequest;
        // to fail since if the number of live brokers is 0, we try to create the offset topic with the default;
        // offsets.topic.replication.factor of 3. The creation will fail since there is not enough live brokers. In order;
        // for the unit test to pass, overriding offsets.topic.replication.factor to 1 for now. When we fix KAFKA-1867, we;
        // need to remove the following config  @Override.
        config.put("offsets.topic.replication.factor", "1");
        String logDirPath = config.getProperty("log.dir");
        logDir = new File(logDirPath);
        time = new MockTime();
        server = TestUtils.createServer(new KafkaConfig(config), time);
        simpleConsumer = new SimpleConsumer("localhost", brokerPort, 1000000, 64 * 1024, "test-client");
        ConsumerMetadataRequest consumerMetadataRequest = new ConsumerMetadataRequest(group);
        boolean continually = true;
        while (continually) {
            ConsumerMetadataResponse consumerMetadataResponse = simpleConsumer.send(consumerMetadataRequest);
            boolean success = consumerMetadataResponse.coordinatorOpt.isPresent();
            if (!success) {
                Thread.sleep(1000);
            }
            continually = !success;
        }
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
    public void testUpdateOffsets() {
        String topic = "topic";

        // Commit an offset;
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, 0);
        Map<Integer, List<Integer>> expectedReplicaAssignment = Sc.toMap(Integer.valueOf("0"), Lists.newArrayList(1));
        // create the topic;
        createTopic(zkClient, topic, expectedReplicaAssignment, Lists.newArrayList(server));

        OffsetCommitRequest commitRequest = new OffsetCommitRequest(group, Sc.toMap(topicAndPartition, new OffsetAndMetadata(42L)));
        OffsetCommitResponse commitResponse = simpleConsumer.commitOffsets(commitRequest);

        Assert.assertEquals(ErrorMapping.NoError, commitResponse.commitStatus.get(topicAndPartition));

        // Fetch it and verify;
        OffsetFetchRequest fetchRequest = new OffsetFetchRequest(group, Lists.newArrayList(topicAndPartition));
        OffsetFetchResponse fetchResponse = simpleConsumer.fetchOffsets(fetchRequest);

        Assert.assertEquals(ErrorMapping.NoError, fetchResponse.requestInfo.get(topicAndPartition).error);
        Assert.assertEquals(OffsetAndMetadata.NoMetadata, fetchResponse.requestInfo.get(topicAndPartition).metadata);
        Assert.assertEquals(new Long(42L), fetchResponse.requestInfo.get(topicAndPartition).offset);

        // Commit a new offset;
        OffsetCommitRequest commitRequest1 = new OffsetCommitRequest(group, Sc.toMap(topicAndPartition, new OffsetAndMetadata(
                100L,
                "some metadata"
        )));
        OffsetCommitResponse commitResponse1 = simpleConsumer.commitOffsets(commitRequest1);

        Assert.assertEquals(ErrorMapping.NoError, commitResponse1.commitStatus.get(topicAndPartition));

        // Fetch it and verify;
        OffsetFetchRequest fetchRequest1 = new OffsetFetchRequest(group, Lists.newArrayList(topicAndPartition));
        OffsetFetchResponse fetchResponse1 = simpleConsumer.fetchOffsets(fetchRequest1);

        Assert.assertEquals(ErrorMapping.NoError, fetchResponse1.requestInfo.get(topicAndPartition).error);
        Assert.assertEquals("some metadata", fetchResponse1.requestInfo.get(topicAndPartition).metadata);
        Assert.assertEquals(new Long(100L), fetchResponse1.requestInfo.get(topicAndPartition).offset);

        // Fetch an unknown topic and verify;
        TopicAndPartition unknownTopicAndPartition = new TopicAndPartition("unknownTopic", 0);
        OffsetFetchRequest fetchRequest2 = new OffsetFetchRequest(group, Lists.newArrayList(unknownTopicAndPartition));
        OffsetFetchResponse fetchResponse2 = simpleConsumer.fetchOffsets(fetchRequest2);

        Assert.assertEquals(OffsetMetadataAndError.UnknownTopicOrPartition, fetchResponse2.requestInfo.get(unknownTopicAndPartition));
        Assert.assertEquals(1, fetchResponse2.requestInfo.size());
    }

    @Test
    public void testCommitAndFetchOffsets() {
        String topic1 = "topic-1";
        String topic2 = "topic-2";
        String topic3 = "topic-3";
        String topic4 = "topic-4";// Topic that group never consumes;
        String topic5 = "topic-5";// Non-existent topic;

        createTopic(zkClient, topic1, 1, null, Lists.newArrayList(server), null);
        createTopic(zkClient, topic2, 2, null, Lists.newArrayList(server), null);
        createTopic(zkClient, topic3, 1, null, Lists.newArrayList(server), null);
        createTopic(zkClient, topic4, 1, null, Lists.newArrayList(server), null);

        OffsetCommitRequest commitRequest = new OffsetCommitRequest("test-group", Sc.toMap(
                new TopicAndPartition(topic1, 0), new OffsetAndMetadata(42L, "metadata one"),
                new TopicAndPartition(topic2, 0), new OffsetAndMetadata(43L, "metadata two"),
                new TopicAndPartition(topic3, 0), new OffsetAndMetadata(44L, "metadata three"),
                new TopicAndPartition(topic2, 1), new OffsetAndMetadata(45L)
        ));
        OffsetCommitResponse commitResponse = simpleConsumer.commitOffsets(commitRequest);
        Assert.assertEquals(ErrorMapping.NoError, commitResponse.commitStatus.get(new TopicAndPartition(topic1, 0)));
        Assert.assertEquals(ErrorMapping.NoError, commitResponse.commitStatus.get(new TopicAndPartition(topic2, 0)));
        Assert.assertEquals(ErrorMapping.NoError, commitResponse.commitStatus.get(new TopicAndPartition(topic3, 0)));
        Assert.assertEquals(ErrorMapping.NoError, commitResponse.commitStatus.get(new TopicAndPartition(topic2, 1)));

        OffsetFetchRequest fetchRequest = new OffsetFetchRequest(group, Lists.newArrayList(
                new TopicAndPartition(topic1, 0),
                new TopicAndPartition(topic2, 0),
                new TopicAndPartition(topic3, 0),
                new TopicAndPartition(topic2, 1),
                new TopicAndPartition(topic3, 1), // An unknown partition;
                new TopicAndPartition(topic4, 0), // An unused topic;
                new TopicAndPartition(topic5, 0)  // An unknown topic;
        ));
        OffsetFetchResponse fetchResponse = simpleConsumer.fetchOffsets(fetchRequest);

        Assert.assertEquals(ErrorMapping.NoError, fetchResponse.requestInfo.get(new TopicAndPartition(topic1, 0)).error);
        Assert.assertEquals(ErrorMapping.NoError, fetchResponse.requestInfo.get(new TopicAndPartition(topic2, 0)).error);
        Assert.assertEquals(ErrorMapping.NoError, fetchResponse.requestInfo.get(new TopicAndPartition(topic3, 0)).error);
        Assert.assertEquals(ErrorMapping.NoError, fetchResponse.requestInfo.get(new TopicAndPartition(topic2, 1)).error);
        Assert.assertEquals(ErrorMapping.UnknownTopicOrPartitionCode, fetchResponse.requestInfo.get(new TopicAndPartition(topic3, 1)).error);
        Assert.assertEquals(ErrorMapping.NoError, fetchResponse.requestInfo.get(new TopicAndPartition(topic4, 0)).error);
        Assert.assertEquals(ErrorMapping.UnknownTopicOrPartitionCode, fetchResponse.requestInfo.get(new TopicAndPartition(topic5, 0)).error);
        Assert.assertEquals(OffsetMetadataAndError.UnknownTopicOrPartition, fetchResponse.requestInfo.get(new TopicAndPartition(topic3, 1)));
        Assert.assertEquals(OffsetMetadataAndError.NoOffset, fetchResponse.requestInfo.get(new TopicAndPartition(topic4, 0)));
        Assert.assertEquals(OffsetMetadataAndError.UnknownTopicOrPartition, fetchResponse.requestInfo.get(new TopicAndPartition(topic5, 0)));

        Assert.assertEquals("metadata one", fetchResponse.requestInfo.get(new TopicAndPartition(topic1, 0)).metadata);
        Assert.assertEquals("metadata two", fetchResponse.requestInfo.get(new TopicAndPartition(topic2, 0)).metadata);
        Assert.assertEquals("metadata three", fetchResponse.requestInfo.get(new TopicAndPartition(topic3, 0)).metadata);
        Assert.assertEquals(OffsetAndMetadata.NoMetadata, fetchResponse.requestInfo.get(new TopicAndPartition(topic2, 1)).metadata);
        Assert.assertEquals(OffsetAndMetadata.NoMetadata, fetchResponse.requestInfo.get(new TopicAndPartition(topic3, 1)).metadata);
        Assert.assertEquals(OffsetAndMetadata.NoMetadata, fetchResponse.requestInfo.get(new TopicAndPartition(topic4, 0)).metadata);
        Assert.assertEquals(OffsetAndMetadata.NoMetadata, fetchResponse.requestInfo.get(new TopicAndPartition(topic5, 0)).metadata);

        Assert.assertEquals(new Long(42L), fetchResponse.requestInfo.get(new TopicAndPartition(topic1, 0)).offset);
        Assert.assertEquals(new Long(43L), fetchResponse.requestInfo.get(new TopicAndPartition(topic2, 0)).offset);
        Assert.assertEquals(new Long(44L), fetchResponse.requestInfo.get(new TopicAndPartition(topic3, 0)).offset);
        Assert.assertEquals(new Long(45L), fetchResponse.requestInfo.get(new TopicAndPartition(topic2, 1)).offset);
        Assert.assertEquals(OffsetAndMetadata.InvalidOffset, fetchResponse.requestInfo.get(new TopicAndPartition(topic3, 1)).offset);
        Assert.assertEquals(OffsetAndMetadata.InvalidOffset, fetchResponse.requestInfo.get(new TopicAndPartition(topic4, 0)).offset);
        Assert.assertEquals(OffsetAndMetadata.InvalidOffset, fetchResponse.requestInfo.get(new TopicAndPartition(topic5, 0)).offset);
    }

    @Test
    public void testLargeMetadataPayload() {
        TopicAndPartition topicAndPartition = new TopicAndPartition("large-metadata", 0);
        Map<Integer, List<Integer>> expectedReplicaAssignment = Sc.toMap(new Integer(0), Lists.newArrayList(1));
        createTopic(zkClient, topicAndPartition.topic, expectedReplicaAssignment, Lists.newArrayList(server));

        OffsetCommitRequest commitRequest = new OffsetCommitRequest("test-group", Sc.toMap(topicAndPartition, new OffsetAndMetadata(
                42L,
                nextString(server.config.offsetMetadataMaxSize)
        )));
        OffsetCommitResponse commitResponse = simpleConsumer.commitOffsets(commitRequest);

        Assert.assertEquals(ErrorMapping.NoError, commitResponse.commitStatus.get(topicAndPartition));

        OffsetCommitRequest commitRequest1 = new OffsetCommitRequest(group, Sc.toMap(topicAndPartition, new OffsetAndMetadata(
                42L,
                nextString(server.config.offsetMetadataMaxSize + 1)
        )));
        OffsetCommitResponse commitResponse1 = simpleConsumer.commitOffsets(commitRequest1);

        Assert.assertEquals(ErrorMapping.OffsetMetadataTooLargeCode, commitResponse1.commitStatus.get(topicAndPartition));

    }
}
