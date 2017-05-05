package kafka.zk;/**
 * Created by zhoulf on 2017/5/5.
 */

import kafka.utils.TestUtils;
import kafka.utils.Utils;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * @author
 * @create 2017-05-05 21 18
 **/
public class EmbeddedZookeeper {
    String connectString;
    File snapshotDir = TestUtils.tempDir();
    File logDir = TestUtils.tempDir();
    Integer tickTime = 500;
    ZooKeeperServer zookeeper;
    NIOServerCnxnFactory factory;

    public EmbeddedZookeeper(String connectString) throws IOException, InterruptedException {
        this.connectString = connectString;
        File snapshotDir = TestUtils.tempDir();
        File logDir = TestUtils.tempDir();
        Integer tickTime = 500;
        ZooKeeperServer zookeeper = new ZooKeeperServer(snapshotDir, logDir, tickTime);
        NIOServerCnxnFactory factory = new NIOServerCnxnFactory();
        factory.configure(new InetSocketAddress("127.0.0.1", org.apache.kafka.common.utils.Utils.getPort(connectString)), 0);
        factory.startup(zookeeper);
    }


    public void shutdown() {
        Utils.swallow(()->zookeeper.shutdown());
        Utils.swallow(()->factory.shutdown());
        Utils.rm(logDir);
        Utils.rm(snapshotDir);
    }

}
