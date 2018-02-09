package kafka.producer;

import kafka.common.ClientIdAllBrokers;
import kafka.common.ClientIdAndBroker;
import kafka.common.ClientIdBroker;
import kafka.func.Handler;
import kafka.utils.Pool;

import java.util.Optional;


/**
 * Tracks metrics of requests made by a given producer client to all brokers.
 * <p>
 * param clientId ClientId of the given producer
 */
public class ProducerRequestStats {
    String clientId;

    public ProducerRequestStats(String clientId) {
        this.clientId = clientId;
        valueFactory = (ClientIdBroker k) -> new ProducerRequestMetrics(k);
        stats = new Pool<>(Optional.of(valueFactory));
        allBrokersStats = new ProducerRequestMetrics(new ClientIdAllBrokers(clientId));
    }

    private Handler<ClientIdBroker, ProducerRequestMetrics> valueFactory ;
    private Pool<ClientIdBroker, ProducerRequestMetrics> stats ;
    private ProducerRequestMetrics allBrokersStats ;

    public ProducerRequestMetrics getProducerRequestAllBrokersStats() {
        return allBrokersStats;
    }

    public ProducerRequestMetrics getProducerRequestStats(String brokerHost, Integer brokerPort) {
        return stats.getAndMaybePut(new ClientIdAndBroker(clientId, brokerHost, brokerPort));
    }
}


