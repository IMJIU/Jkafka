package kafka.utils;

import kafka.api.LeaderAndIsr;
import kafka.api.LeaderIsrAndControllerEpoch;
import kafka.func.Tuple;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.data.Stat;

import java.util.Optional;
import java.util.logging.Logger;

/**
 * @author zhoulf
 * @create 2017-10-20 30 17
 **/

public class ReplicationUtils {
    static Logging logger = Logging.getLogger(ReplicationUtils.class.getName());

    public static Tuple<Boolean, Integer> updateLeaderAndIsr(ZkClient zkClient, String topic, Integer partitionId, LeaderAndIsr newLeaderAndIsr, Integer controllerEpoch, Integer zkVersion) {
//        logger.debug(String.format("Updated ISR for partition <%s,%d> to %s", topic, partitionId, newLeaderAndIsr.isr));
//        String path = ZkUtils.getTopicPartitionLeaderAndIsrPath(topic, partitionId);
//        String newLeaderData = ZkUtils.leaderAndIsrZkData(newLeaderAndIsr, controllerEpoch);
//        // use the epoch of the controller that made the leadership decision, instead of the current controller epoch;
//        return ZkUtils.conditionalUpdatePersistentPath(zkClient, path, newLeaderData, zkVersion, Some(checkLeaderAndIsrZkData));
        return null;
    }
//
//    public Tuple<Boolean, Integer> checkLeaderAndIsrZkData(ZkClient zkClient, String path, String expectedLeaderAndIsrInfo) {
//        try {
//            Tuple<Optional<String>, Stat> writtenLeaderAndIsrInfo = ZkUtils.readDataMaybeNull(zkClient, path);
//            Optional<String> writtenLeaderOpt = writtenLeaderAndIsrInfo.v1;
//            Stat writtenStat = writtenLeaderAndIsrInfo.v2;
//            Optional<LeaderIsrAndControllerEpoch> expectedLeader = parseLeaderAndIsr(expectedLeaderAndIsrInfo, path, writtenStat);
//            writtenLeaderOpt match {
//                case Some(writtenData) =>
//                    val writtenLeader = parseLeaderAndIsr(writtenData, path, writtenStat);
//                    (expectedLeader, writtenLeader)match {
//                    case (Some(expectedLeader),Some(writtenLeader)) =>
//                        if (expectedLeader == writtenLeader)
//                            return (true,writtenStat.getVersion());
//                    case _ =>
//                }
//                case None =>
//            }
//        } catch  (Throwable throwable) {
//            throwable.printStackTrace();
//        }
//        return Tuple.of(false, -1);
//    }
//
//    public Optional<LeaderIsrAndControllerEpoch> getLeaderIsrAndEpochForPartition(ZkClient zkClient, String topic, Int partition) {
//        val leaderAndIsrPath = ZkUtils.getTopicPartitionLeaderAndIsrPath(topic, partition);
//        val leaderAndIsrInfo = ZkUtils.readDataMaybeNull(zkClient, leaderAndIsrPath);
//        val leaderAndIsrOpt = leaderAndIsrInfo._1;
//        val stat = leaderAndIsrInfo._2;
//        leaderAndIsrOpt match {
//            case Some(leaderAndIsrStr) =>parseLeaderAndIsr(leaderAndIsrStr, leaderAndIsrPath, stat);
//            case None =>None;
//        }
//    }

//    private Optional<LeaderIsrAndControllerEpoch> parseLeaderAndIsr(String leaderAndIsrStr, String path, Stat stat) {
//        Json.parseFull(leaderAndIsrStr) match {
//            case Some(m) =>
//                val leaderIsrAndEpochInfo = m.asInstanceOf < Map < String, Any>>
//                val leader = leaderIsrAndEpochInfo.get("leader").get.asInstanceOf < Integer >
//                        val epoch = leaderIsrAndEpochInfo.get("leader_epoch").get.asInstanceOf < Integer >
//                    val isr = leaderIsrAndEpochInfo.get("isr").get.asInstanceOf < List < Int >>
//                    val controllerEpoch = leaderIsrAndEpochInfo.get("controller_epoch").get.asInstanceOf < Integer >
//                    val zkPathVersion = stat.getVersion;
//                debug(String.format("Leader %d, Epoch %d, Isr %s, Zk path version %d for leaderAndIsrPath %s", leader, epoch,
//                        isr.toString(), zkPathVersion, path));
//                Some(LeaderIsrAndControllerEpoch(LeaderAndIsr(leader, epoch, isr, zkPathVersion), controllerEpoch));
//            case None =>None;
//        }
//    }

}
