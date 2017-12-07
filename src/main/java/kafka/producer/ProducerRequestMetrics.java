package kafka.producer;

import com.google.common.collect.ImmutableMap;
import com.yammer.metrics.core.Histogram;
import kafka.common.ClientIdAllBrokers;
import kafka.common.ClientIdAndBroker;
import kafka.common.ClientIdBroker;
import kafka.metrics.KafkaMetricsGroup;
import kafka.metrics.KafkaTimer;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author zhoulf
 * @create 2017-12-05 17:36
 **/
public class ProducerRequestMetrics extends KafkaMetricsGroup {
    public ClientIdBroker metricId;
    public Map<String, String> tags;
    public KafkaTimer requestTimer;
    public Histogram requestSizeHist;

    public ProducerRequestMetrics(ClientIdBroker metricId) {
        this.metricId = metricId;
        if (metricId instanceof ClientIdAndBroker) {
            ClientIdAndBroker clientIdAndTopic = (ClientIdAndBroker) metricId;
            tags = ImmutableMap.of("clientId", clientIdAndTopic.clientId, "brokerHost", clientIdAndTopic.brokerHost, "brokerPort", clientIdAndTopic.brokerPort.toString());
        } else {
            ClientIdAllBrokers clientIdAndTopic = (ClientIdAllBrokers) metricId;
            tags = ImmutableMap.of("clientId", clientIdAndTopic.clientId);
        }
        requestTimer = new KafkaTimer(newTimer("ProducerRequestRateAndTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS, tags));
        requestSizeHist = newHistogram("ProducerRequestSize", true, tags);
    }
}