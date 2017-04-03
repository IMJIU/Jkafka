package kafka.server;

import kafka.func.Converter;
import kafka.utils.Pool;

import java.util.Optional;

/**
 * Created by Administrator on 2017/4/2.
 */
public class BrokerTopicStats {
    private static Converter<String,BrokerTopicMetrics> valueFactory = (k) -> new BrokerTopicMetrics(Optional.of(k));
    private static Pool<String, BrokerTopicMetrics> stats = new Pool<>(Optional.of(valueFactory));
    private static BrokerTopicMetrics allTopicsStats = new BrokerTopicMetrics(Optional.empty());

    public static BrokerTopicMetrics getBrokerAllTopicsStats(){
        return allTopicsStats;
    }

    public static BrokerTopicMetrics getBrokerTopicStats(String topic ){
        return stats.getAndMaybePut(topic);
    }
}
