package kafka.controller.ctrl;

/**
 * @author zhoulf
 * @create 2017-11-02 16:18
 **/
public class PartitionAndReplica {
    public String topic;
    public Integer partition;
    public Integer replica;

    public PartitionAndReplica(String topic, Integer partition, Integer replica) {
        this.topic = topic;
        this.partition = partition;
        this.replica = replica;
    }

    @Override
    public String toString() {
        return String.format("<Topic=%s,Partition=%d,Replica=%d>", topic, partition, replica);
    }
}
