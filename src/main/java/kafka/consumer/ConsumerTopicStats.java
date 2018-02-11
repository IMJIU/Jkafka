package kafka.consumer;

import com.google.common.collect.ImmutableMap;
import com.yammer.metrics.core.Meter;
import kafka.annotation.threadsafe;
import kafka.common.ClientIdAllTopics;
import kafka.common.ClientIdAndTopic;
import kafka.common.ClientIdTopic;
import kafka.func.Handler;
import kafka.metrics.KafkaMetricsGroup;
import kafka.server.ClientIdTopicPartition;
import kafka.utils.Logging;
import kafka.utils.Pool;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * @author zhoulf
 * @create 2017-10-30 31 17
 **/

@threadsafe
class ConsumerTopicMetrics extends KafkaMetricsGroup {
    public ClientIdTopic metricId;
    public Meter messageRate;
    public Meter byteRate;

    public ConsumerTopicMetrics(ClientIdTopic metricId) {
        this.metricId = metricId;
        Map<String, String> tags;
        if (metricId instanceof ClientIdAndTopic) {
            ClientIdAndTopic clientIdAndTopic = (ClientIdAndTopic) metricId;
            tags = ImmutableMap.of("clientId", clientIdAndTopic.clientId, "topic", clientIdAndTopic.topic);
        } else {
            ClientIdAllTopics clientIdAndTopic = (ClientIdAllTopics) metricId;
            tags = ImmutableMap.of("clientId", clientIdAndTopic.clientId);
        }
        messageRate = newMeter("MessagesPerSec", "messages", TimeUnit.SECONDS, tags);
        byteRate = newMeter("BytesPerSec", "bytes", TimeUnit.SECONDS, tags);
    }


}

/**
 * Tracks metrics for each topic the given consumer client has consumed data from.
 * <p>
 * clientId The clientId of the given consumer client.
 */
public class ConsumerTopicStats extends Logging {
    public String clientId;
    private Handler<ClientIdAndTopic, ConsumerTopicMetrics> valueFactory = k -> new ConsumerTopicMetrics(k);
    private Pool<ClientIdAndTopic, ConsumerTopicMetrics> stats = new Pool<>(Optional.of(valueFactory));
    private ConsumerTopicMetrics allTopicStats ; // to differentiate from a topic named AllTopics;

    public ConsumerTopicStats(String clientId) {
        this.clientId = clientId;
        allTopicStats = new ConsumerTopicMetrics(new ClientIdAllTopics(clientId));
    }

    public ConsumerTopicMetrics getConsumerAllTopicStats() {
        return allTopicStats;
    }

    public ConsumerTopicMetrics getConsumerTopicStats(String topic) {
        return stats.getAndMaybePut(new ClientIdAndTopic(clientId, topic));
    }
}


