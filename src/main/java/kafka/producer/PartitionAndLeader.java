package kafka.producer;

import java.util.Optional;

/**
 * Created by Administrator on 2017/12/7.
 */
public class PartitionAndLeader {
    public String topic;
    public Integer partitionId;
    public Optional<Integer> leaderBrokerIdOpt;

    public PartitionAndLeader(String topic, Integer partitionId, Optional<Integer> leaderBrokerIdOpt) {
        this.topic = topic;
        this.partitionId = partitionId;
        this.leaderBrokerIdOpt = leaderBrokerIdOpt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PartitionAndLeader that = (PartitionAndLeader) o;

        if (topic != null ? !topic.equals(that.topic) : that.topic != null) return false;
        if (partitionId != null ? !partitionId.equals(that.partitionId) : that.partitionId != null) return false;
        return leaderBrokerIdOpt != null ? leaderBrokerIdOpt.equals(that.leaderBrokerIdOpt) : that.leaderBrokerIdOpt == null;
    }

    @Override
    public int hashCode() {
        int result = topic != null ? topic.hashCode() : 0;
        result = 31 * result + (partitionId != null ? partitionId.hashCode() : 0);
        result = 31 * result + (leaderBrokerIdOpt != null ? leaderBrokerIdOpt.hashCode() : 0);
        return result;
    }
}
