package kafka.javaapi;

import kafka.cluster.Broker;
import kafka.utils.Sc;

import java.util.List;

/**
 * @author zhoulf
 * @create 2017-12-19 18 20
 **/

public class TopicMetadata {
    private kafka.api.TopicMetadata underlying;

    public TopicMetadata(kafka.api.TopicMetadata underlying) {
        this.underlying = underlying;
    }

    public String topic() {
        return underlying.topic;
    }

    public List<PartitionMetadata> partitionsMetadata() {
        return Sc.map(underlying.partitionsMetadata, p -> new PartitionMetadata(p));
    }

    public Short errorCode() {
        return underlying.errorCode;
    }

    public Integer sizeInBytes() {
        return underlying.sizeInBytes();
    }

    @Override
    public String toString() {
        return underlying.toString();
    }
}


class PartitionMetadata {
    private kafka.api.PartitionMetadata underlying;

    public PartitionMetadata(kafka.api.PartitionMetadata underlying) {
        this.underlying = underlying;
        leader = underlying.leader;
    }

    public Integer partitionId() {
        return underlying.partitionId;
    }


    public Broker leader;

    public List<Broker> replicas() {
        return underlying.replicas;
    }

    public List<Broker> isr() {
        return underlying.isr;
    }

    public Short errorCode() {
        return underlying.errorCode;
    }

    public Integer sizeInBytes() {
        return underlying.sizeInBytes();
    }

    @Override
    public String toString() {
        return underlying.toString();
    }
}
