package kafka.server;

import kafka.cluster.Partition;
import kafka.log.LogManager;
import kafka.utils.MockScheduler;
import kafka.utils.MockTime;
import kafka.utils.Sc;
import kafka.utils.TestUtils;
import org.I0Itec.zkclient.ZkClient;
import org.easymock.EasyMock;
import org.junit.Test;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author zhoulf
 * @create 2018-01-10 24 17
 **/

public class ReplicaManagerTest  {

    String topic = "test-topic";

    @Test
   public void testHighWaterMarkDirectoryMapping() {
        Properties props = TestUtils.createBrokerConfig(1,null,null);
        KafkaConfig config = new KafkaConfig(props);
        ZkClient zkClient = EasyMock.createMock(ZkClient.class);
        LogManager mockLogMgr = TestUtils.createLogManager(Sc.map(config.logDirs, l->new File(l)),null,null,null);
         MockTime time = new MockTime();
        ReplicaManager rm = new ReplicaManager(config, time, zkClient, new MockScheduler(time), mockLogMgr, new AtomicBoolean(false));
        Partition partition = rm.getOrCreatePartition(topic, 1);
        partition.getOrCreateReplica(1);
        rm.checkpointHighWatermarks();
    }

    @Test
   public void testHighwaterMarkRelativeDirectoryMapping() {
        Properties props = TestUtils.createBrokerConfig(1,null,null);
        props.put("log.dir", TestUtils.tempRelativeDir("data").getAbsolutePath());
        KafkaConfig config = new KafkaConfig(props);
        ZkClient zkClient = EasyMock.createMock(ZkClient.class);
        LogManager mockLogMgr = TestUtils.createLogManager(Sc.map(config.logDirs, l->new File(l)),null,null,null);
        MockTime time = new MockTime();
        ReplicaManager rm = new ReplicaManager(config, time, zkClient, new MockScheduler(time), mockLogMgr, new AtomicBoolean(false));
        Partition partition = rm.getOrCreatePartition(topic, 1);
        partition.getOrCreateReplica(1);
        rm.checkpointHighWatermarks();
    }
}
