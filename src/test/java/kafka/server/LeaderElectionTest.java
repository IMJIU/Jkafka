package kafka.server;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import kafka.api.*;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.controller.ControllerChannelManager;
import kafka.controller.ctrl.ControllerContext;
import kafka.controller.ctrl.LeaderIsrAndControllerEpoch;
import kafka.func.ActionP;
import kafka.log.TopicAndPartition;
import kafka.utils.*;
import kafka.zk.ZooKeeperTestHarness;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

/**
 * @author zhoulf
 * @create 2018-01-10 40 16
 **/

public class LeaderElectionTest extends ZooKeeperTestHarness {
    Logging logging = Logging.getLogger(LeaderElectionTest.class.getName());
    int brokerId1 = 0;
    int brokerId2 = 1;

    int port1 = TestUtils.choosePort();
    int port2 = TestUtils.choosePort();

    Properties configProps1 = TestUtils.createBrokerConfig(brokerId1, port1, false);
    Properties configProps2 = TestUtils.createBrokerConfig(brokerId2, port2, false);
    List<KafkaServer> servers = Lists.newArrayList();

    boolean staleControllerEpochDetected = false;

    @Before
    public void setUp() throws IOException, InterruptedException {
        super.setUp();
        // start both servers;
        KafkaServer server1 = TestUtils.createServer(new KafkaConfig(configProps1));
        KafkaServer server2 = TestUtils.createServer(new KafkaConfig(configProps2));
        servers.addAll(Lists.newArrayList(server1, server2));
    }
//
//     @Override
//     public void tearDown() {
//        servers.forEach(server -> server.shutdown());
//        servers.forEach(server -> Utils.rm(server.config.logDirs));
//        super.tearDown();
//    }

    @Test
    public void testLeaderElectionAndEpoch() throws Throwable {
        // start 2 brokers;
        String topic = "new-topic";
        int partitionId = 0;

        // create topic with 1 partition, 2 replicas, one on each broker;
        Optional<Integer> leader1 = TestUtils.createTopic(zkClient, topic, ImmutableMap.of(0, Lists.newArrayList(0, 1)), servers).get(0);

        Integer leaderEpoch1 = ZkUtils.getEpochForPartition(zkClient, topic, partitionId);
        logging.debug("leader Epoc: " + leaderEpoch1);
        logging.debug(String.format("Leader is elected to be: %s", leader1.orElse(-1)));
        Assert.assertTrue("Leader should get elected", leader1.isPresent());
        // this NOTE is to avoid transient test failures;
        Assert.assertTrue("Leader could be broker 0 or broker 1", (leader1.orElse(-1) == 0) || (leader1.orElse(-1) == 1));
        Assert.assertEquals("First epoch value should be 0", new Integer(0), leaderEpoch1);

        // kill the server hosting the preferred replica;
        servers.get(1).shutdown();
        // check if leader moves to the other server;
        Optional<Integer> oldLeaderOpt;
        if (leader1.get() == 0) {
            oldLeaderOpt = Optional.empty();
        } else {
            oldLeaderOpt = leader1;
        }
        Optional<Integer> leader2 = TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId, null, oldLeaderOpt, null);
        Integer leaderEpoch2 = ZkUtils.getEpochForPartition(zkClient, topic, partitionId);
        logging.debug(String.format("Leader is elected to be: %s", leader1.orElse(-1)));
        logging.debug("leader Epoc: " + leaderEpoch2);
        Assert.assertEquals("Leader must move to broker 0", new Integer(0), leader2.orElse(-1));
        if (leader1.get() == leader2.get())
            Assert.assertEquals("Second epoch value should be " + leaderEpoch1 + 1, new Integer(leaderEpoch1 + 1), leaderEpoch2);
        else
            Assert.assertEquals(String.format("Second epoch value should be %d", leaderEpoch1 + 1), new Integer(leaderEpoch1 + 1), leaderEpoch2);

        servers.get(1).startup();
        servers.get(0).shutdown();
        Thread.sleep(zookeeper.tickTime);
        if (leader2.get() == 1) {
            oldLeaderOpt = Optional.empty();
        } else {
            oldLeaderOpt = leader2;
        }
        Optional<Integer> leader3 = TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, partitionId, null, oldLeaderOpt, null);
        Integer leaderEpoch3 = ZkUtils.getEpochForPartition(zkClient, topic, partitionId);
        logging.debug("leader Epoc: " + leaderEpoch3);
        logging.debug(String.format("Leader is elected to be: %s", leader3.orElse(-1)));
        Assert.assertEquals("Leader must return to 1", new Integer(1), leader3.orElse(-1));
        if (leader2.get() == leader3.get())
            Assert.assertEquals("Second epoch value should be " + leaderEpoch2, leaderEpoch2, leaderEpoch3);
        else
            Assert.assertEquals(String.format("Second epoch value should be %d", leaderEpoch2 + 1), new Integer(leaderEpoch2 + 1), leaderEpoch3);
    }

    @Test
    public void testLeaderElectionWithStaleControllerEpoch() throws Throwable {
        // start 2 brokers;
        String topic = "new-topic";
        Integer partitionId = 0;

        // create topic with 1 partition, 2 replicas, one on each broker;
        Optional<Integer> leader1 = TestUtils.createTopic(zkClient, topic, ImmutableMap.of(0, Lists.newArrayList(0, 1)), servers).get(0);

        Integer leaderEpoch1 = ZkUtils.getEpochForPartition(zkClient, topic, partitionId);
        System.out.println("leader Epoc: " + leaderEpoch1);
        System.out.println(String.format("Leader is elected to be: %s", leader1.orElse(-1)));
        Assert.assertTrue("Leader should get elected", leader1.isPresent());
        // this NOTE is to avoid transient test failures;
        Assert.assertTrue("Leader could be broker 0 or broker 1", (leader1.orElse(-1) == 0) || (leader1.orElse(-1) == 1));
        Assert.assertEquals("First epoch value should be 0", 0L, leaderEpoch1.longValue());

        // start another controller;
        Integer controllerId = 2;
        KafkaConfig controllerConfig = new KafkaConfig(TestUtils.createBrokerConfig(controllerId, TestUtils.choosePort(), true));
        Set<Broker> brokers = Sc.mapToSet(servers, s -> new Broker(s.config.brokerId, "localhost", s.config.port));
        ControllerContext controllerContext = new ControllerContext(zkClient, 6000);
        controllerContext.liveBrokers_(brokers);
        ControllerChannelManager controllerChannelManager = new ControllerChannelManager(controllerContext, controllerConfig);
        controllerChannelManager.startup();
        Integer staleControllerEpoch = 0;
        HashMap<TopicAndPartition, LeaderIsrAndControllerEpoch> leaderAndIsr = new HashMap<>();
        leaderAndIsr.put(new TopicAndPartition(topic, partitionId),
                new LeaderIsrAndControllerEpoch(new LeaderAndIsr(brokerId2, Lists.newArrayList(brokerId1, brokerId2)), 2));
        Map partitionStateInfo = Sc.mapValue(leaderAndIsr, l -> new PartitionStateInfo(l, Sets.newHashSet(0, 1)));
        LeaderAndIsrRequest leaderAndIsrRequest = new LeaderAndIsrRequest(partitionStateInfo, brokers, controllerId,
                staleControllerEpoch, 0, "");

        controllerChannelManager.sendRequest(brokerId2, leaderAndIsrRequest, staleControllerEpochCallback);
        TestUtils.waitUntilTrue(() -> staleControllerEpochDetected == true,"Controller epoch should be stale");
        Assert.assertTrue("Stale controller epoch not detected by the broker", staleControllerEpochDetected);

        controllerChannelManager.shutdown();
    }

    ActionP<RequestOrResponse> staleControllerEpochCallback = (RequestOrResponse response) -> {
        LeaderAndIsrResponse leaderAndIsrResponse = (LeaderAndIsrResponse) response;
        if (leaderAndIsrResponse.errorCode == ErrorMapping.StaleControllerEpochCode) {
            staleControllerEpochDetected = true;
        }
    };
}
