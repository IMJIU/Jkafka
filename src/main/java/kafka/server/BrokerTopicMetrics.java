package kafka.server;

import com.google.common.collect.Maps;
import com.yammer.metrics.core.Meter;
import kafka.metrics.KafkaMetricsGroup;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class BrokerTopicMetrics extends KafkaMetricsGroup {
    public Optional<String> name;
    public Meter messagesInRate;
    public Meter bytesInRate;
    public Meter bytesOutRate;
    public Meter bytesRejectedRate;
    public Meter failedProduceRequestRate;
    public Meter failedFetchRequestRate;

    public Map<String, String> tags = Maps.newHashMap();

    public BrokerTopicMetrics(Optional<String> name) {
        this.name = name;
        if (name.isPresent()) {
            tags.put("topic", name.get());
        }
        messagesInRate = newMeter("MessagesInPerSec", "messages", TimeUnit.SECONDS, tags);
        bytesInRate = newMeter("BytesInPerSec", "bytes", TimeUnit.SECONDS, tags);
        bytesOutRate = newMeter("BytesOutPerSec", "bytes", TimeUnit.SECONDS, tags);
        bytesRejectedRate = newMeter("BytesRejectedPerSec", "bytes", TimeUnit.SECONDS, tags);
        failedProduceRequestRate = newMeter("FailedProduceRequestsPerSec", "requests", TimeUnit.SECONDS, tags);
        failedFetchRequestRate = newMeter("FailedFetchRequestsPerSec", "requests", TimeUnit.SECONDS, tags);
    }

}
