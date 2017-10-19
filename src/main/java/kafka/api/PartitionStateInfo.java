package kafka.api;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

/**
 * @author zhoulf
 * @create 2017-10-19 27 11
 **/

public class PartitionStateInfo {
    public LeaderIsrAndControllerEpoch leaderIsrAndControllerEpoch;
    public Set<Integer> allReplicas;
    public Integer replicationFactor;

    public PartitionStateInfo(LeaderIsrAndControllerEpoch leaderIsrAndControllerEpoch, Set<Integer> allReplicas) {
        this.leaderIsrAndControllerEpoch = leaderIsrAndControllerEpoch;
        this.allReplicas = allReplicas;
        replicationFactor = allReplicas.size();
    }

    public static PartitionStateInfo readFrom(ByteBuffer buffer) {
        int controllerEpoch = buffer.getInt();
        int leader = buffer.getInt();
        int leaderEpoch = buffer.getInt();
        int isrSize = buffer.getInt();
        List<Integer> isr = Lists.newArrayList();
        for (int i = 0; i < isrSize; i++) {
            isr.add(buffer.getInt());
        }
        int zkVersion = buffer.getInt();
        int replicationFactor = buffer.getInt();
        Set<Integer> replicas = Sets.newHashSet();
        for (int i = 0; i < replicationFactor; i++) {
            replicas.add(buffer.getInt());
        }
        return new PartitionStateInfo(
                new LeaderIsrAndControllerEpoch(
                        new LeaderAndIsr(leader, leaderEpoch, isr, zkVersion), controllerEpoch),
                replicas);
    }

    public void writeTo(ByteBuffer buffer) {
        buffer.putInt(leaderIsrAndControllerEpoch.controllerEpoch);
        buffer.putInt(leaderIsrAndControllerEpoch.leaderAndIsr.leader);
        buffer.putInt(leaderIsrAndControllerEpoch.leaderAndIsr.leaderEpoch);
        buffer.putInt(leaderIsrAndControllerEpoch.leaderAndIsr.isr.size());
        leaderIsrAndControllerEpoch.leaderAndIsr.isr.forEach(r -> buffer.putInt(r));
        buffer.putInt(leaderIsrAndControllerEpoch.leaderAndIsr.zkVersion);
        buffer.putInt(replicationFactor);
        allReplicas.forEach(r -> buffer.putInt(r));
    }

    public Integer sizeInBytes() {
        Integer size =
                4 /* epoch of the controller that elected the leader */ +
                        4 /* leader broker id */ +
                        4 /* leader epoch */ +
                        4 /* number of replicas in isr */ +
                        4 * leaderIsrAndControllerEpoch.leaderAndIsr.isr.size() /* replicas in isr */ +
                        4 /* zk version */ +
                        4 /* replication factor */ +
                        allReplicas.size() * 4;
        return size;
    }

    @Override
    public String toString() {
        StringBuilder partitionStateInfo = new StringBuilder();
        partitionStateInfo.append("(LeaderAndIsrInfo:" + leaderIsrAndControllerEpoch.toString());
        partitionStateInfo.append(",ReplicationFactor:" + replicationFactor + ")");
        partitionStateInfo.append(",AllReplicas:" + allReplicas + ")");
        return partitionStateInfo.toString();
    }
}
