package kafka.producer;

import com.google.common.collect.Sets;
import kafka.api.TopicMetadata;
import kafka.cluster.Broker;
import kafka.common.UnavailableProducerException;
import kafka.utils.Logging;

import java.util.*;

/**
 * @author zhoulf
 * @create 2017-12-01 25 18
 **/


public class ProducerPool extends Logging {
    ProducerConfig config;
    private HashMap<Integer, SyncProducer> syncProducers = new HashMap<Integer, SyncProducer>();
    private Object lock = new Object();

    public ProducerPool(ProducerConfig config) {
        this.config = config;
    }

    /**
     * Used in ProducerPool to initiate a SyncProducer connection with a broker.
     */
    public static SyncProducer createSyncProducer(ProducerConfig config, Broker broker) {
        Properties props = new Properties();
        props.put("host", broker.host);
        props.put("port", broker.port.toString());
        props.putAll(config.props.props);
        return new SyncProducer(new SyncProducerConfig(props));
    }

    public void updateProducer(List<TopicMetadata> topicMetadata) {
        HashSet<Broker> newBrokers = Sets.newHashSet();
        topicMetadata.forEach(tmd -> {
            tmd.partitionsMetadata.forEach(pmd -> {
                if (pmd.leader != null)
                    newBrokers.add(pmd.leader);
            });
        });
        synchronized (lock) {
            newBrokers.forEach(b -> {
                if (syncProducers.containsKey(b.id)) {
                    syncProducers.get(b.id).close();
                    syncProducers.put(b.id, ProducerPool.createSyncProducer(config, b));
                } else
                    syncProducers.put(b.id, ProducerPool.createSyncProducer(config, b));
            });
        }
    }

    public SyncProducer getProducer(Integer brokerId) {
        synchronized (lock) {
            SyncProducer producer = syncProducers.get(brokerId);
            if (producer != null) {
                return producer;
            } else {
                throw new UnavailableProducerException(String.format("Sync producer for broker id %d does not exist", brokerId));
            }
        }
    }

    /**
     * Closes all the producers in the pool
     */
    public void close() {
        synchronized (lock) {
            info("Closing all sync producers");
            Iterator<SyncProducer> iter = syncProducers.values().iterator();
            while (iter.hasNext())
                iter.next().close();
        }
    }
}
