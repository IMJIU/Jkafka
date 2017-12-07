package kafka.producer;

import kafka.func.Handler;
import kafka.utils.Pool;
import java.util.Optional;


/**
 * Stores metrics of serialization and message sending activity of each producer client in a (clientId -> ProducerStats) map.
 */
public class ProducerStatsRegistry {
    private  final static Handler<String, ProducerStats> valueFactory = (String k) -> new ProducerStats(k);
    private  final static Pool<String, ProducerStats> statsRegistry = new Pool<>(Optional.of(valueFactory));

    public static ProducerStats getProducerStats(String clientId) {
        return statsRegistry.getAndMaybePut(clientId);
    }

    public static  void removeProducerStats(String clientId) {
        statsRegistry.remove(clientId);
    }
}
