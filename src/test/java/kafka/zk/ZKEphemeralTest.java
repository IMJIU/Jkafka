package kafka.zk;

import kafka.consumer.ConsumerConfig;
import kafka.utils.TestUtils;
import kafka.utils.ZKStringSerializer;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by Administrator on 2017/12/17.
 */
public class ZKEphemeralTest extends ZooKeeperTestHarness {
    public static final int zkSessionTimeoutMs = 1000;

    @Test
    public void testEphemeralNodeCleanup() {
        ConsumerConfig config = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnect, "test", "1"));
        ZkClient zkClient = new ZkClient(zkConnect, zkSessionTimeoutMs, config.zkConnectionTimeoutMs, new ZKStringSerializer());

        try {
            ZkUtils.createEphemeralPathExpectConflict(zkClient, "/tmp/zktest", "node created");
        } catch (Exception e) {
        }

        String testData = null;
        testData = ZkUtils.readData(zkClient, "/tmp/zktest").v1;
        System.out.println(testData);
        Assert.assertNotNull(testData);
        zkClient.close();
        zkClient = new ZkClient(zkConnect, zkSessionTimeoutMs, config.zkConnectionTimeoutMs, new ZKStringSerializer());
        boolean nodeExists = ZkUtils.pathExists(zkClient, "/tmp/zktest");
        Assert.assertFalse(nodeExists);
    }
}

