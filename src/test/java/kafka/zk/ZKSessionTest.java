package kafka.zk;

import kafka.consumer.ConsumerConfig;
import kafka.utils.TestUtils;
import kafka.utils.ZKStringSerializer;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.Watcher;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Administrator on 2017/12/17.
 */
public class ZKSessionTest extends ZooKeeperTestHarness {
    @Before
    public void setUp() throws IOException, InterruptedException {
        zookeeper = new EmbeddedZookeeper(zkConnect);

    }

    @Test
    public void testEphemeralNodeCleanup() throws Exception {
        ExecutorService exe = Executors.newFixedThreadPool(10);

        exe.submit(() -> {
            for (int i = 0; i < 100; i++) {
                int num = i;
                exe.submit(() -> {
                    System.out.println("coming :"+num);
                    zkClient = new ZkClient(zookeeper.connectString, zkSessionTimeout, zkConnectionTimeout, new ZKStringSerializer());
                    zkClient.subscribeStateChanges(new SessionExpirationListener());
                    try {
                        Thread.sleep(100000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                });
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        zkClient = new ZkClient(zookeeper.connectString, zkSessionTimeout, zkConnectionTimeout, new ZKStringSerializer());
        zkClient.subscribeStateChanges(new SessionExpirationListener());
        Thread.sleep(100000);

    }
}


class SessionExpirationListener implements IZkStateListener {
    /**
     * Called after the zookeeper session has expired and a new session has been created. You would have to re-create
     * any ephemeral nodes here.
     *
     * @throws Exception On any error.
     */
//    @throws(classOf<Exception>)
    public void handleNewSession() {
        System.out.println("ZK expired; shut down all controller components and try to re-elect");
    }

    @Override
    public void handleStateChanged(Watcher.Event.KeeperState state) throws Exception {
        // TODO Auto-generated method stub
        System.out.println("handleStateChanged");
    }

}