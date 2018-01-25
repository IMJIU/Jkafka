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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PartitionAndReplica that = (PartitionAndReplica) o;

        if (topic != null ? !topic.equals(that.topic) : that.topic != null) return false;
        if (partition != null ? !partition.equals(that.partition) : that.partition != null) return false;
        return replica != null ? replica.equals(that.replica) : that.replica == null;

    }

    @Override
    public int hashCode() {
        int result = topic != null ? topic.hashCode() : 0;
        result = 31 * result + (partition != null ? partition.hashCode() : 0);
        result = 31 * result + (replica != null ? replica.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return String.format("<Topic=%s,Partition=%d,Replica=%d>", topic, partition, replica);
    }
}
