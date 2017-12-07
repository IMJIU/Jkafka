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
public class ProducerTopicMetrics extends KafkaMetricsGroup {
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




