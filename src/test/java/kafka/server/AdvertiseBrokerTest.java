package kafka.server;

import kafka.api.*;
import kafka.cluster.Broker;
import kafka.cluster.Partition;
import kafka.cluster.Replica;
import kafka.common.*;
import kafka.controller.KafkaController;
import kafka.log.Log;
import kafka.log.LogManager;
import kafka.log.TopicAndPartition;
import kafka.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.network.RequestChannel;
import kafka.utils.*;
import kafka.zk.ZooKeeperTestHarness;
import org.I0Itec.zkclient.ZkClient;
import org.easymock.EasyMock;
import org.junit.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

import static kafka.utils.TestUtils.*;

public class AdvertiseBrokerTest extends ZooKeeperTestHarness {
    KafkaServer server = null;
    Integer brokerId = 0;
    String advertisedHostName = "routable-host";
    Integer advertisedPort = 1234;

    @Override
    public void setUp() throws IOException, InterruptedException {
        super.setUp();
        Properties props = TestUtils.createBrokerConfig(brokerId, TestUtils.choosePort(), null);
        props.put("advertised.host.name", advertisedHostName);
        props.put("advertised.port", advertisedPort.toString());
        server = TestUtils.createServer(new KafkaConfig(props));
    }

//    @Override
//    public void tearDown() {
//        server.shutdown();
//        Utils.rm(server.config.logDirs);
//        super.tearDown();
//    }

    @Test
    public void testBrokerAdvertiseToZK() {
        Optional<Broker> brokerInfo = ZkUtils.getBrokerInfo(zkClient, brokerId);
        Assert.assertEquals(advertisedHostName, brokerInfo.get().host);
        Assert.assertEquals(advertisedPort, brokerInfo.get().port);
    }
}
