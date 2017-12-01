package kafka.producer;

import com.google.common.collect.ImmutableMap;
import com.yammer.metrics.core.Meter;
import kafka.annotation.threadsafe;
import kafka.common.ClientIdAllTopics;
import kafka.common.ClientIdAndTopic;
import kafka.common.ClientIdTopic;
import kafka.func.Handler;
import kafka.metrics.KafkaMetricsGroup;
import kafka.utils.Pool;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * @author zhoulf
 * @create 2017-12-01 22 10
 **/

@threadsafe
class ProducerTopicMetrics extends KafkaMetricsGroup {
    public ClientIdTopic metricId;
    public Map<String, String> tags;
    public Meter messageRate;
    public Meter byteRate;
    public Meter droppedMessageRate;

    public ProducerTopicMetrics(ClientIdTopic metricId) {
        this.metricId = metricId;

        if (metricId instanceof ClientIdAndTopic) {
            ClientIdAndTopic clientIdAndTopic = (ClientIdAndTopic) metricId;
            tags = ImmutableMap.of("clientId", clientIdAndTopic.clientId, "topic", clientIdAndTopic.topic);
        } else {
            ClientIdAllTopics clientIdAndTopic = (ClientIdAllTopics) metricId;
            tags = ImmutableMap.of("clientId", clientIdAndTopic.clientId);
        }
        messageRate = newMeter("MessagesPerSec", "messages", TimeUnit.SECONDS, tags);
        byteRate = newMeter("BytesPerSec", "bytes", TimeUnit.SECONDS, tags);
        droppedMessageRate = newMeter("DroppedMessagesPerSec", "drops", TimeUnit.SECONDS, tags);
    }
}


/**
 * Tracks metrics for each topic the given producer client has produced data to.
 *
 * param clientId The clientId of the given producer client.
 */
class ProducerTopicStats {
    public String clientId;

    public ProducerTopicStats(String clientId) {
        this.clientId = clientId;
    }

    private Handler<ClientIdTopic, ProducerTopicMetrics> valueFactory = (ClientIdTopic k) -> new ProducerTopicMetrics(k);
    private Pool<ClientIdTopic, ProducerTopicMetrics> stats = new Pool<ClientIdTopic, ProducerTopicMetrics>(Optional.of(valueFactory));
    private ProducerTopicMetrics allTopicsStats = new ProducerTopicMetrics(new ClientIdAllTopics(clientId)); // to differentiate from a topic named AllTopics;

    public ProducerTopicMetrics getProducerAllTopicsStats() {
        return allTopicsStats;
    }

    public ProducerTopicMetrics getProducerTopicStats(String topic) {
        return stats.getAndMaybePut(new ClientIdAndTopic(clientId, topic));
    }
}

/**
 * Stores the topic stats information of each producer client in a (clientId -> ProducerTopicStats) map.
 */
class ProducerTopicStatsRegistry {
    private static Handler<String, ProducerTopicStats> valueFactory = (String k) -> new ProducerTopicStats(k);
    private static Pool<String, ProducerTopicStats> globalStats = new Pool<String, ProducerTopicStats>(Optional.of(valueFactory));

    public  static ProducerTopicStats getProducerTopicStats(String clientId) {
       return  globalStats.getAndMaybePut(clientId);
    }

    public static void removeProducerTopicStats(String clientId) {
        globalStats.remove(clientId);
    }
}
