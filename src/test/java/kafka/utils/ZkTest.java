package kafka.utils;

import kafka.func.Tuple;
import kafka.zk.ZooKeeperTestHarness;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

import java.util.Optional;

/**
 * @author zhoulf
 * @create 2018-01-16 14:08
 **/
public class ZkTest  extends ZooKeeperTestHarness {

    @Test
    public void t01(){
        String zkPath = "/brokers/topics/foo";
//        String data = "{\"version\":1,\"partitions\":{\"0\":[0,1]}}";
//        ZkUtils.createPersistentPath(zkClient, zkPath, data);
        Tuple<Optional<String>, Stat> result = ZkUtils.readDataMaybeNull(zkClient,zkPath);
    }
}
