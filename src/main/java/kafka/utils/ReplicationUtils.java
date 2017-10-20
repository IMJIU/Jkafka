package kafka.utils;

/**
 * @author zhoulf
 * @create 2017-10-20 30 17
 **/

public class ReplicationUtils extends Logging {

       public void updateLeaderAndIsr(ZkClient zkClient, String topic, Int partitionId, LeaderAndIsr newLeaderAndIsr, Int controllerEpoch,
        Int zkVersion): (Boolean,Int) = {
        debug(String.format("Updated ISR for partition <%s,%d> to %s",topic, partitionId, newLeaderAndIsr.isr.mkString(",")))
        val path = ZkUtils.getTopicPartitionLeaderAndIsrPath(topic, partitionId);
        val newLeaderData = ZkUtils.leaderAndIsrZkData(newLeaderAndIsr, controllerEpoch);
        // use the epoch of the controller that made the leadership decision, instead of the current controller epoch;
        ZkUtils.conditionalUpdatePersistentPath(zkClient, path, newLeaderData, zkVersion, Some(checkLeaderAndIsrZkData));
        }

       public void checkLeaderAndIsrZkData(ZkClient zkClient, String path, String expectedLeaderAndIsrInfo): (Boolean,Int) = {
        try {
        val writtenLeaderAndIsrInfo = ZkUtils.readDataMaybeNull(zkClient, path);
        val writtenLeaderOpt = writtenLeaderAndIsrInfo._1;
        val writtenStat = writtenLeaderAndIsrInfo._2;
        val expectedLeader = parseLeaderAndIsr(expectedLeaderAndIsrInfo, path, writtenStat);
        writtenLeaderOpt match {
        case Some(writtenData) =>
        val writtenLeader = parseLeaderAndIsr(writtenData, path, writtenStat);
        (expectedLeader,writtenLeader) match {
        case (Some(expectedLeader),Some(writtenLeader)) =>
        if(expectedLeader == writtenLeader)
        return (true,writtenStat.getVersion());
        case _ =>
        }
        case None =>
        }
        } catch {
        case Exception e1 =>
        }
        (false,-1);
        }

       public void getLeaderIsrAndEpochForPartition(ZkClient zkClient, String topic, Int partition):Option<LeaderIsrAndControllerEpoch> = {
        val leaderAndIsrPath = ZkUtils.getTopicPartitionLeaderAndIsrPath(topic, partition);
        val leaderAndIsrInfo = ZkUtils.readDataMaybeNull(zkClient, leaderAndIsrPath);
        val leaderAndIsrOpt = leaderAndIsrInfo._1;
        val stat = leaderAndIsrInfo._2;
        leaderAndIsrOpt match {
        case Some(leaderAndIsrStr) => parseLeaderAndIsr(leaderAndIsrStr, leaderAndIsrPath, stat);
        case None => None;
        }
        }

private void parseLeaderAndIsr(String leaderAndIsrStr, String path, Stat stat);
        : Option<LeaderIsrAndControllerEpoch> = {
        Json.parseFull(leaderAndIsrStr) match {
        case Some(m) =>
        val leaderIsrAndEpochInfo = m.asInstanceOf<Map<String, Any>>
        val leader = leaderIsrAndEpochInfo.get("leader").get.asInstanceOf<Integer>
        val epoch = leaderIsrAndEpochInfo.get("leader_epoch").get.asInstanceOf<Integer>
        val isr = leaderIsrAndEpochInfo.get("isr").get.asInstanceOf<List<Int>>
        val controllerEpoch = leaderIsrAndEpochInfo.get("controller_epoch").get.asInstanceOf<Integer>
        val zkPathVersion = stat.getVersion;
        debug(String.format("Leader %d, Epoch %d, Isr %s, Zk path version %d for leaderAndIsrPath %s",leader, epoch,
        isr.toString(), zkPathVersion, path));
        Some(LeaderIsrAndControllerEpoch(LeaderAndIsr(leader, epoch, isr, zkPathVersion), controllerEpoch));
        case None => None;
        }
        }

        }
