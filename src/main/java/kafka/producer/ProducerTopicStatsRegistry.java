package kafka.producer;

import kafka.func.Handler;
import kafka.utils.Pool;

import java.util.Optional;

/**
 * Stores the topic stats information of each producer client in a (clientId -> ProducerTopicStats) map.
 */
public class ProducerTopicStatsRegistry {
    private static Handler<String, ProducerTopicStats> valueFactory = (String k) -> new ProducerTopicStats(k);
    private static Pool<String, ProducerTopicStats> globalStats = new Pool<String, ProducerTopicStats>(Optional.of(valueFactory));

    public static ProducerTopicStats getProducerTopicStats(String clientId) {
        return globalStats.getAndMaybePut(clientId);
    }

    public static void removeProducerTopicStats(String clientId) {
        globalStats.remove(clientId);
    }
}
