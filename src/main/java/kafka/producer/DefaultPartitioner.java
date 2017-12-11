package kafka.producer;

import kafka.utils.Utils;
import kafka.utils.VerifiableProperties;

import java.util.Random;

/**
 * @author zhoulf
 * @create 2017-12-11 11:08
 **/
public class DefaultPartitioner implements Partitioner {
    public VerifiableProperties props;
    private Random random = new java.util.Random();

    public DefaultPartitioner(VerifiableProperties props) {
        this.props = props;
    }


    @Override
    public Integer partition(Object key, Integer numPartitions) {
        return Utils.abs(key.hashCode()) % numPartitions;
    }
}
