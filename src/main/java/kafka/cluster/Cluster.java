package kafka.cluster;

import com.google.common.collect.Maps;

import java.util.HashMap;
import java.util.Optional;

/**
 * @author zhoulf
 * @create 2017-11-01 33 14
 **/
public class Cluster {

    private HashMap<Integer, Broker> brokers = Maps.newHashMap();

    public Cluster() {

    }

    public Cluster(Iterable<Broker> brokerList) {
        for (Broker broker : brokerList)
            brokers.put(broker.id, broker);
    }

    public Broker getBroker(Integer id) {
        return brokers.get(id);
    }

    public void add(Broker broker) {
        brokers.put(broker.id, broker);
    }

    public void remove(Integer id) {
        brokers.remove(id);
    }

    public int size() {
        return brokers.size();
    }

    @Override
    public String toString() {
        return "Cluster(" + brokers + ")";
    }
}
