package kafka.producer;

import com.google.common.collect.ImmutableMap;
import com.yammer.metrics.core.Histogram;
import kafka.common.*;
import kafka.func.Handler;
import kafka.metrics.KafkaMetricsGroup;
import kafka.metrics.KafkaTimer;
import kafka.utils.Pool;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author zhoulf
 * @create 2017-12-01 32 18
 **/

class ProducerRequestMetrics extends KafkaMetricsGroup {
    public ClientIdBroker metricId;
    public Map<String, String> tags;
    public KafkaTimer requestTimer;
    public Histogram requestSizeHist;
    public ProducerRequestMetrics(ClientIdBroker metricId) {
        this.metricId = metricId;
        if (metricId instanceof ClientIdAndBroker) {
            ClientIdAndBroker clientIdAndTopic = (ClientIdAndBroker) metricId;
            tags = ImmutableMap.of("clientId", clientIdAndTopic.clientId, "brokerHost", clientIdAndTopic.brokerHost,"brokerPort" , clientIdAndTopic.brokerPort.toString());
        } else {
            ClientIdAllBrokers clientIdAndTopic = (ClientIdAllBrokers) metricId;
            tags = ImmutableMap.of("clientId", clientIdAndTopic.clientId);
        }
        requestTimer =  new KafkaTimer(newTimer("ProducerRequestRateAndTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS, tags));
        requestSizeHist = newHistogram("ProducerRequestSize",  true, tags);
    }


/**
 * Tracks metrics of requests made by a given producer client to all brokers.
 * @param clientId ClientId of the given producer
 */
class ProducerRequestStats{
    String clientId;

    public ProducerRequestStats(String clientId) {
        this.clientId = clientId;
    }

    private Handler<ClientIdBroker,ProducerRequestMetrics> valueFactory = (ClientIdBroker k) -> new ProducerRequestMetrics(k);
private Pool<ClientIdBroker, ProducerRequestMetrics> stats = new Pool<ClientIdBroker, ProducerRequestMetrics>(Some(valueFactory));
private val allBrokersStats = new ProducerRequestMetrics(new ClientIdAllBrokers(clientId));

       public ProducerRequestMetrics  void getProducerRequestAllBrokersStats() allBrokersStats;

       public ProducerRequestMetrics  void getProducerRequestStats(String brokerHost, Int brokerPort) {
        stats.getAndMaybePut(new ClientIdAndBroker(clientId, brokerHost, brokerPort));
        }
        }

/**
 * Stores the request stats information of each producer client in a (clientId -> ProducerRequestStats) map.
 */
        object ProducerRequestStatsRegistry {
private val valueFactory = (String k) -> new ProducerRequestStats(k);
private val globalStats = new Pool<String, ProducerRequestStats>(Some(valueFactory));

       public void getProducerRequestStats(String clientId) = {
        globalStats.getAndMaybePut(clientId);
        }

       public void removeProducerRequestStats(String clientId) {
        globalStats.remove(clientId);
        }
        }
