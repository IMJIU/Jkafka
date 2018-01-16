package kafka.log;/**
 * Created by zhoulf on 2017/4/1.
 */

import kafka.cluster.Partition;
import kafka.cluster.Replica;
import kafka.func.Tuple;

/**
 * @author
 * @create 2017-04-01 17:49
 **/
public class TopicAndPartition {
    public String topic;
    public Integer partition;

    public TopicAndPartition(String topic, Integer partition) {
        this.topic = topic;
        this.partition = partition;
    }

    public TopicAndPartition(Partition partition) {
        this.topic = partition.topic;
        this.partition = partition.partitionId;
    }

    public TopicAndPartition(Replica replica) {
        this.topic = replica.topic;
        this.partition = replica.partitionId;
    }


    public Tuple<String,Integer> asTuple() {
        return Tuple.of(topic, partition);
    }

    @Override
    public String toString() {
        return String.format("[%s,%d]", topic, partition);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TopicAndPartition that = (TopicAndPartition) o;

        if (topic != null ? !topic.equals(that.topic) : that.topic != null) return false;
        return partition != null ? partition.equals(that.partition) : that.partition == null;
    }

    @Override
    public int hashCode() {
        int result = topic != null ? topic.hashCode() : 0;
        result = 31 * result + (partition != null ? partition.hashCode() : 0);
        return result;
    }
}
