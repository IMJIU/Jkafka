package kafka.consumer;

import com.google.common.collect.ImmutableMap;
import com.yammer.metrics.core.Histogram;
import kafka.common.ClientIdAllBrokers;
import kafka.common.ClientIdAndBroker;
import kafka.common.ClientIdBroker;
import kafka.func.Handler;
import kafka.metrics.KafkaMetricsGroup;
import kafka.metrics.KafkaTimer;
import kafka.utils.Pool;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author zhoulf
 * @create 2017-10-27 45 16
 **/

 class FetchRequestAndResponseMetrics extends KafkaMetricsGroup {
    public ClientIdBroker metricId;
    public Map<String, String> tags;
    public KafkaTimer requestTimer;
    public Histogram requestSizeHist;

    public FetchRequestAndResponseMetrics(ClientIdBroker metricId) {
        this.metricId = metricId;
        if (metricId instanceof  ClientIdAndBroker) {
            ClientIdAndBroker metric = (ClientIdAndBroker)metricId;
            tags = ImmutableMap.of("clientId", metric.clientId, "brokerHost",
                    metric.brokerHost, "brokerPort", metric.brokerPort.toString());
        } else if(metricId instanceof  ClientIdAllBrokers){
            tags = ImmutableMap.of("clientId", ((ClientIdAllBrokers) metricId).clientId);
        }
        requestTimer = new KafkaTimer(newTimer("FetchRequestRateAndTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS, tags));
        requestSizeHist = newHistogram("FetchResponseSize", true, tags);
    }
}

    /**
     * Tracks metrics of the requests made by a given consumer client to all brokers, and the responses obtained from the brokers.
     *
     * @param clientId ClientId of the given consumer
     */
    class FetchRequestAndResponseStats {
        public String clientId;
        private Handler<ClientIdBroker, FetchRequestAndResponseMetrics> valueFactory = k -> new FetchRequestAndResponseMetrics(k);
        private Pool<ClientIdBroker, FetchRequestAndResponseMetrics> stats = new Pool(Optional.of(valueFactory));
        private FetchRequestAndResponseMetrics allBrokersStats;

        public FetchRequestAndResponseStats(String clientId) {
            this.clientId = clientId;
            allBrokersStats = new FetchRequestAndResponseMetrics(new ClientIdAllBrokers(clientId));
        }

        public FetchRequestAndResponseMetrics getFetchRequestAndResponseAllBrokersStats() {
            return allBrokersStats;
        }

        public FetchRequestAndResponseMetrics getFetchRequestAndResponseStats(String brokerHost, Integer brokerPort) {
            return stats.getAndMaybePut(new ClientIdAndBroker(clientId, brokerHost, brokerPort));
        }
    }

    /**
     * Stores the fetch request and response stats information of each consumer client in a (clientId -> FetchRequestAndResponseStats) map.
     */
    class FetchRequestAndResponseStatsRegistry {
        private static  Handler<String,FetchRequestAndResponseStats> valueFactory = k->new  FetchRequestAndResponseStats(k);

        private static  Pool<String, FetchRequestAndResponseStats> globalStats = new Pool(Optional.of(valueFactory));

        public static FetchRequestAndResponseStats getFetchRequestAndResponseStats(String clientId){
            return globalStats.getAndMaybePut(clientId);
        }

        public void removeConsumerFetchRequestAndResponseStats(String clientId) {
            String pattern = (".*" + clientId + ".*");
            Set<String> keys = globalStats.keys();
            for (String key:keys) {
                if(pattern.matches(key)){
                    globalStats.remove(key);
                }
            }
        }
    }
