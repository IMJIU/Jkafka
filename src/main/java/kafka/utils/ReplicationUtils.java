package kafka.utils;

import com.alibaba.fastjson.JSON;
import kafka.api.LeaderAndIsr;
import kafka.controller.ctrl.LeaderIsrAndControllerEpoch;
import kafka.func.Tuple;
import kafka.func.Tuple3;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author zhoulf
 * @create 2017-10-20 30 17
 **/

public class ReplicationUtils {
    static Logging logger = Logging.getLogger(ReplicationUtils.class.getName());

    public static Tuple<Boolean, Integer> updateLeaderAndIsr(ZkClient zkClient, String topic, Integer partitionId, LeaderAndIsr newLeaderAndIsr, Integer controllerEpoch, Integer zkVersion) {
        logger.debug(String.format("Updated ISR for partition <%s,%d> to %s", topic, partitionId, newLeaderAndIsr.isr));
        String path = ZkUtils.getTopicPartitionLeaderAndIsrPath(topic, partitionId);
        String newLeaderData = ZkUtils.leaderAndIsrZkData(newLeaderAndIsr, controllerEpoch);
        // use the epoch of the controller that made the leadership decision, instead of the current controller epoch;
        return ZkUtils.conditionalUpdatePersistentPath(zkClient, path, newLeaderData, zkVersion,
                Optional.of((Tuple3<ZkClient, String, String> p1) -> {
                    try {
                        ZkClient zk = p1.v1;
                        String pathString = p1.v2;
                        String expectedLeaderAndIsrInfo = p1.v3;
                        Tuple<Optional<String>, Stat> writtenLeaderAndIsrInfo = ZkUtils.readDataMaybeNull(zk, pathString);
                        Optional<String> writtenLeaderOpt = writtenLeaderAndIsrInfo.v1;
                        Stat writtenStat = writtenLeaderAndIsrInfo.v2;
                        Optional<LeaderIsrAndControllerEpoch> expectedLeader = parseLeaderAndIsr(expectedLeaderAndIsrInfo, pathString, writtenStat);
                        if (writtenLeaderOpt.isPresent()) {
                            Optional<LeaderIsrAndControllerEpoch> writtenLeader = parseLeaderAndIsr(writtenLeaderOpt.get(), pathString, writtenStat);
                            if (writtenLeader.isPresent() && expectedLeader.isPresent()) {
                                if (expectedLeader == writtenLeader)
                                    return Tuple.of(true, writtenStat.getVersion());
                            }
                        }
                    } catch (Throwable throwable) {
                        throwable.printStackTrace();
                    }
                    return Tuple.of(false, -1);
                }));
    }

//
//    public static Tuple<Boolean, Integer> checkLeaderAndIsrZkData(ZkClient zkClient, String path, String expectedLeaderAndIsrInfo) {
//        try {
//            Tuple<Optional<String>, Stat> writtenLeaderAndIsrInfo = ZkUtils.readDataMaybeNull(zkClient, path);
//            Optional<String> writtenLeaderOpt = writtenLeaderAndIsrInfo.v1;
//            Stat writtenStat = writtenLeaderAndIsrInfo.v2;
//            Optional<LeaderIsrAndControllerEpoch> expectedLeader = parseLeaderAndIsr(expectedLeaderAndIsrInfo, path, writtenStat);
//            if (writtenLeaderOpt.isPresent()) {
//                Optional<LeaderIsrAndControllerEpoch> writtenLeader = parseLeaderAndIsr(writtenLeaderOpt.get(), path, writtenStat);
//                if (writtenLeader.isPresent() && expectedLeader.isPresent()) {
//                    if (expectedLeader == writtenLeader)
//                        return Tuple.of(true, writtenStat.getVersion());
//                }
//            }
//        } catch (Throwable throwable) {
//            throwable.printStackTrace();
//        }
//        return Tuple.of(false, -1);
//    }

    public static Optional<LeaderIsrAndControllerEpoch> getLeaderIsrAndEpochForPartition(ZkClient zkClient, String topic, Integer partition) {
        String leaderAndIsrPath = ZkUtils.getTopicPartitionLeaderAndIsrPath(topic, partition);
        Tuple<Optional<String>, Stat> leaderAndIsrInfo = ZkUtils.readDataMaybeNull(zkClient, leaderAndIsrPath);
        Optional<String> leaderAndIsrOpt = leaderAndIsrInfo.v1;
        Stat stat = leaderAndIsrInfo.v2;
        if (leaderAndIsrOpt.isPresent()) {
            return parseLeaderAndIsr(leaderAndIsrOpt.get(), leaderAndIsrPath, stat);
        } else {
            return Optional.empty();
        }
    }

    private static Optional<LeaderIsrAndControllerEpoch> parseLeaderAndIsr(String leaderAndIsrStr, String path, Stat stat) {
        // TODO: 2017/11/1 json??  Json.parseFull(leaderAndIsrStr) match {case Some(m) =>
        Map<String, Object> leaderIsrAndEpochInfo = JSON.parseObject(leaderAndIsrStr, Map.class);
        if (leaderIsrAndEpochInfo != null) {
            Integer leader = (Integer) leaderIsrAndEpochInfo.get("leader");
            Integer epoch = (Integer) leaderIsrAndEpochInfo.get("leader_epoch");
            List<Integer> isr = (List<Integer>) leaderIsrAndEpochInfo.get("isr");
            Integer controllerEpoch = (Integer) leaderIsrAndEpochInfo.get("controller_epoch");
            int zkPathVersion = stat.getVersion();
            logger.debug(String.format("Leader %d, Epoch %d, Isr %s, Zk path version %d for leaderAndIsrPath %s", leader, epoch, isr.toString(), zkPathVersion, path));
            return Optional.of(new LeaderIsrAndControllerEpoch(new LeaderAndIsr(leader, epoch, isr, zkPathVersion), controllerEpoch));
        } else {
            return Optional.empty();
        }
    }

}
