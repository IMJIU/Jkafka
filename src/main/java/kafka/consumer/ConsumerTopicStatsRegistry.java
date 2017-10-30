package kafka.consumer;

/**
 * @author zhoulf
 * @create 2017-10-30 17:33
 **/

import kafka.func.Handler;
import kafka.utils.Pool;

import java.util.Optional;

/**
 * Stores the topic stats information of each consumer client in a (clientId -> ConsumerTopicStats) map.
 */
public class ConsumerTopicStatsRegistry {
    private static Handler<String,ConsumerTopicStats> valueFactory = k-> new ConsumerTopicStats(k);
    private static Pool<String, ConsumerTopicStats> globalStats = new Pool<>(Optional.of(valueFactory));

    public static ConsumerTopicStats getConsumerTopicStat(String clientId) {
        return globalStats.getAndMaybePut(clientId);
    }

    public static void removeConsumerTopicStat(String clientId) {
        globalStats.remove(clientId);
    }
}