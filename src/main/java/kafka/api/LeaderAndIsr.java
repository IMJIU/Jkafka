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
    public List<Integer> isr;
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LeaderAndIsr that = (LeaderAndIsr) o;

        if (leader != null ? !leader.equals(that.leader) : that.leader != null) return false;
        if (leaderEpoch != null ? !leaderEpoch.equals(that.leaderEpoch) : that.leaderEpoch != null) return false;
        if (isr != null ? !isr.equals(that.isr) : that.isr != null) return false;
        return zkVersion != null ? zkVersion.equals(that.zkVersion) : that.zkVersion == null;
    }

    @Override
    public int hashCode() {
        int result = leader != null ? leader.hashCode() : 0;
        result = 31 * result + (leaderEpoch != null ? leaderEpoch.hashCode() : 0);
        result = 31 * result + (isr != null ? isr.hashCode() : 0);
        result = 31 * result + (zkVersion != null ? zkVersion.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(ImmutableMap.of("leader", leader, "leader_epoch", leaderEpoch, "isr", isr));
    }
}
