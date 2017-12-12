package kafka.zk;

import kafka.utils.Logging;
import kafka.utils.TestZKUtils;
import kafka.utils.Utils;
import kafka.utils.ZKStringSerializer;
import org.I0Itec.zkclient.ZkClient;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

/**
 * @author zhoulf
 * @create 2017-11-27 15:17
 **/

public abstract class ZooKeeperTestHarness  {
    public String zkConnect = TestZKUtils.zookeeperConnect();
    public EmbeddedZookeeper zookeeper = null;
    public ZkClient zkClient = null;
    public int zkConnectionTimeout = 6000;
    public int zkSessionTimeout = 6000;

    @Before
    public void setUp() throws IOException, InterruptedException {
        zookeeper = new EmbeddedZookeeper(zkConnect);
        zkClient = new ZkClient(zookeeper.connectString, zkSessionTimeout, zkConnectionTimeout, new ZKStringSerializer());
    }

    @After
    public void tearDown() {
        Utils.swallow(() -> zkClient.close());
        Utils.swallow(() -> zookeeper.shutdown());
    }

}
