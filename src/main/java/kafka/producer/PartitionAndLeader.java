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
}
