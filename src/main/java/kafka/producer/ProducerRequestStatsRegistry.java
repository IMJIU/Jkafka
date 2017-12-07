package kafka.producer;

import kafka.func.Handler;
import kafka.utils.Pool;

import java.util.Optional;

/**
 * Stores the request stats information of each producer client in a (clientId -> ProducerRequestStats) map.
 */
public class ProducerRequestStatsRegistry {
    private static Handler<String, ProducerRequestStats> valueFactory = (String k) -> new ProducerRequestStats(k);
    private static Pool<String, ProducerRequestStats> globalStats = new Pool<>(Optional.of(valueFactory));

    public static ProducerRequestStats getProducerRequestStats(String clientId) {
        return globalStats.getAndMaybePut(clientId);
    }

    public void removeProducerRequestStats(String clientId) {
        globalStats.remove(clientId);
    }
}