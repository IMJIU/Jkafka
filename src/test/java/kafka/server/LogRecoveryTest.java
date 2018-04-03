package kafka.server;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import kafka.log.OffsetCheckpoint;
import kafka.log.TopicAndPartition;
import kafka.producer.DefaultPartitioner;
import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.serializer.StringEncoder;
import kafka.utils.IntEncoder;
import kafka.utils.Sc;
import kafka.utils.TestUtils;
import kafka.zk.ZooKeeperTestHarness;
import org.junit.Assert;
import org.junit.Test;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import static kafka.utils.TestUtils.createTopic;
import static kafka.utils.TestUtils.waitUntilLeaderIsElectedOrChanged;

public class LogRecoveryTest extends ZooKeeperTestHarness {

    List<KafkaConfig> configs;


    String topic = "new-topic";
    Integer partitionId = 0;

    KafkaServer server1 = null;
    KafkaServer server2 = null;

    KafkaConfig configProps1;
    KafkaConfig configProps2;

    String message = "hello";

    Producer<Integer, String> producer;
    OffsetCheckpoint hwFile1;
    OffsetCheckpoint hwFile2;
    List<KafkaServer> servers = Lists.newArrayList();

    @Override
    public void setUp() throws IOException, InterruptedException {
        super.setUp();
        configs = Sc.map(TestUtils.createBrokerConfigs(2, null), p -> {
            KafkaConfig config = new KafkaConfig(p);
            config.replicaLagTimeMaxMs = 5000L;
            config.replicaLagMaxMessages = 10L;
            config.replicaFetchWaitMaxMs = 100;
            config.replicaLagMaxMessages = 20L;
            return config;
        });
        configProps1 = Sc.head(configs);
        configProps2 = Sc.last(configs);
        hwFile1 = new OffsetCheckpoint(new File(configProps1.logDirs.get(0), ReplicaManager.HighWatermarkFilename));
        hwFile2 = new OffsetCheckpoint(new File(configProps2.logDirs.get(0), ReplicaManager.HighWatermarkFilename));
        // start both servers;
        server1 = TestUtils.createServer(configProps1);
        server2 = TestUtils.createServer(configProps2);
        servers.addAll(Lists.newArrayList(server1, server2));

        // create topic with 1 partition, 2 replicas, one on each broker;
        createTopic(zkClient, topic, ImmutableMap.of(0, Lists.newArrayList(0, 1)), servers);

        // create the producer;
        producer = TestUtils.createProducer(TestUtils.getBrokerListStrFromConfigs(configs),
                StringEncoder.class.getName(),
                IntEncoder.class.getName(), DefaultPartitioner.class.getName(), null);
    }

//   @Override
//   public void tearDown() {
//    producer.close();
//    for(server <- servers) {
//      server.shutdown();
//      Utils.rm(server.config.logDirs(0));
//    }
//    super.tearDown();
//  }

    @Test
    public void testHWCheckpointNoFailuresSingleLogSegment() {
        Long numMessages = 2L;
        sendMessages(numMessages.intValue());

        // give some time for the follower 1 to record leader HW;
        TestUtils.waitUntilTrue(() -> server2.replicaManager.getReplica(topic, 0).get().highWatermark().messageOffset.equals(numMessages),
                "Failed to update high watermark for follower after timeout");

        servers.forEach(server -> server.replicaManager.checkpointHighWatermarks());
        Long leaderHW = hwFile1.read().getOrDefault(new TopicAndPartition(topic, 0), 0L);
        Assert.assertEquals(numMessages, leaderHW);
        Long followerHW = hwFile2.read().getOrDefault(new TopicAndPartition(topic, 0), 0L);
        Assert.assertEquals(numMessages, followerHW);
    }

    @Test
    public void testHWCheckpointWithFailuresSingleLogSegment() throws InterruptedException {
        Optional<Integer> leader = waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId, null, null, null);

        Assert.assertEquals(new Long(0), hwFile1.read().getOrDefault(new TopicAndPartition(topic, 0), 0L));

        sendMessages(1);
        Thread.sleep(1000);
        Long hw = 1L;

        // kill the server hosting the preferred replica;
        server1.shutdown();
        Assert.assertEquals(hw, hwFile1.read().getOrDefault(new TopicAndPartition(topic, 0), 0L));

        // check if leader moves to the other server;
        leader = waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId, null, leader, null);
        Assert.assertEquals("Leader must move to broker 1", new Integer(1), leader.orElse(new Integer(-1)));

        // bring the preferred replica back;
        server1.startup();

        leader = waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId, null, null, null);
        Assert.assertTrue("Leader must remain on broker 1, in case of zookeeper session expiration it can move to broker 0",
                leader.isPresent() && (leader.get() == 0 || leader.get() == 1));

        Assert.assertEquals(hw, hwFile1.read().getOrDefault(new TopicAndPartition(topic, 0), 0L));
        // since server 2 was never shut down, the hw value of 30 is probably not checkpointed to disk yet;
        server2.shutdown();
        Assert.assertEquals(hw, hwFile2.read().getOrDefault(new TopicAndPartition(topic, 0), 0L));

        server2.startup();
        leader = waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId, null, leader, null);
        Assert.assertTrue("Leader must remain on broker 0, in case of zookeeper session expiration it can move to broker 1",
                leader.isPresent() && (leader.get() == 0 || leader.get() == 1));

        sendMessages(1);
        hw += 1;
        final Long f_hw = hw;
        // give some time for follower 1 to record leader HW of 60;
        TestUtils.waitUntilTrue(() ->
                        server2.replicaManager.getReplica(topic, 0).get().highWatermark().messageOffset.equals(f_hw),
                "Failed to update high watermark for follower after timeout");
        // shutdown the servers to allow the hw to be checkpointed;
        servers.forEach(server -> server.shutdown());
        Assert.assertEquals(hw, hwFile1.read().getOrDefault(new TopicAndPartition(topic, 0), 0L));
        Assert.assertEquals(hw, hwFile2.read().getOrDefault(new TopicAndPartition(topic, 0), 0L));
    }

    @Test
    public void testHWCheckpointNoFailuresMultipleLogSegments()

    {
        sendMessages(20);
        Long hw = 20L;
        // give some time for follower 1 to record leader HW of 600;
        TestUtils.waitUntilTrue(() ->
                        server2.replicaManager.getReplica(topic, 0).get().highWatermark().messageOffset.equals(hw),
                "Failed to update high watermark for follower after timeout");
        // shutdown the servers to allow the hw to be checkpointed;
        servers.forEach(server -> server.shutdown());
        Long leaderHW = hwFile1.read().getOrDefault(new TopicAndPartition(topic, 0), 0L);
        Assert.assertEquals(hw, leaderHW);
        Long followerHW = hwFile2.read().getOrDefault(new TopicAndPartition(topic, 0), 0L);
        Assert.assertEquals(hw, followerHW);
    }

    public void testHWCheckpointWithFailuresMultipleLogSegments() {
        Optional<Integer> leader = waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId, null, null, null);

        sendMessages(2);
        Long hw = 2L;
        final Long f_hw = hw;
        // allow some time for the follower to get the leader HW;
        TestUtils.waitUntilTrue(() ->
                        server2.replicaManager.getReplica(topic, 0).get().highWatermark().messageOffset.equals(f_hw),
                "Failed to update high watermark for follower after timeout");
        // kill the server hosting the preferred replica;
        server1.shutdown();
        server2.shutdown();
        Assert.assertEquals(hw, hwFile1.read().getOrDefault(new TopicAndPartition(topic, 0), 0L));
        Assert.assertEquals(hw, hwFile2.read().getOrDefault(new TopicAndPartition(topic, 0), 0L));

        server2.startup();
        // check if leader moves to the other server;
        leader = waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId, null, leader, null);
        Assert.assertEquals("Leader must move to broker 1", new Integer(1), leader.orElse(-1));

        Assert.assertEquals(hw, hwFile1.read().getOrDefault(new TopicAndPartition(topic, 0), 0L));

        // bring the preferred replica back;
        server1.startup();

        Assert.assertEquals(hw, hwFile1.read().getOrDefault(new TopicAndPartition(topic, 0), 0L));
        Assert.assertEquals(hw, hwFile2.read().getOrDefault(new TopicAndPartition(topic, 0), 0L));

        sendMessages(2);
        hw += 2;
        Long f_hw2 = hw;
        // allow some time for the follower to get the leader HW;
        TestUtils.waitUntilTrue(() ->
                        server1.replicaManager.getReplica(topic, 0).get().highWatermark().messageOffset.equals(f_hw2),
                "Failed to update high watermark for follower after timeout");
        // shutdown the servers to allow the hw to be checkpointed;
        servers.forEach(server -> server.shutdown());
        Assert.assertEquals(hw, hwFile1.read().getOrDefault(new TopicAndPartition(topic, 0), 0L));
        Assert.assertEquals(hw, hwFile2.read().getOrDefault(new TopicAndPartition(topic, 0), 0L));
    }

    private void sendMessages(Integer n) {
        if (n == null) {
            n = 1;
        }
        for (int i = 0; i < n; i++) {
            producer.send(new KeyedMessage<>(topic, 0, message));
        }
    }
}
