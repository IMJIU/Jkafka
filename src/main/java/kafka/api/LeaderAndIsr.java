package kafka.api;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.ImmutableMap;

import java.util.List;

public class LeaderAndIsr {

    public static Integer initialLeaderEpoch = 0;
    public static Integer initialZKVersion = 0;
    public static Integer NoLeader = -1;
    public static Integer LeaderDuringDelete = -2;

    public Integer leader;
    public Integer leaderEpoch;
    public java.util.List<Integer> isr;
    public Integer zkVersion;

    public LeaderAndIsr(Integer leader, Integer leaderEpoch, List<Integer> isr, Integer zkVersion) {
        this.leader = leader;
        this.leaderEpoch = leaderEpoch;
        this.isr = isr;
        this.zkVersion = zkVersion;
    }

    public LeaderAndIsr(Integer leader, List<Integer> isr) {
        this(leader, LeaderAndIsr.initialLeaderEpoch, isr, LeaderAndIsr.initialZKVersion);
    }

    @Override
    public String toString() {
        return JSON.toJSONString(ImmutableMap.of("leader", leader, "leader_epoch", leaderEpoch, "isr", isr));
    }
}
