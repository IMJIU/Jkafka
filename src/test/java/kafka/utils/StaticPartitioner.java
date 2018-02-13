package kafka.utils;

import kafka.producer.Partitioner;

public class StaticPartitioner implements Partitioner {
    VerifiableProperties props = null;

    public StaticPartitioner(VerifiableProperties props) {
        this.props = props;
    }

    public Integer partition(Object data, Integer numPartitions) {
        return (((String) data).length() % numPartitions);
    }
}