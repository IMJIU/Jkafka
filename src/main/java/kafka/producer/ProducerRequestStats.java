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
    }

    private Handler<ClientIdBroker, ProducerRequestMetrics> valueFactory = (ClientIdBroker k) -> new ProducerRequestMetrics(k);
    private Pool<ClientIdBroker, ProducerRequestMetrics> stats = new Pool<ClientIdBroker, ProducerRequestMetrics>(Optional.of(valueFactory));
    private ProducerRequestMetrics allBrokersStats = new ProducerRequestMetrics(new ClientIdAllBrokers(clientId));

    public ProducerRequestMetrics getProducerRequestAllBrokersStats() {
        return allBrokersStats;
    }

    public ProducerRequestMetrics getProducerRequestStats(String brokerHost, Integer brokerPort) {
        return stats.getAndMaybePut(new ClientIdAndBroker(clientId, brokerHost, brokerPort));
    }
}


