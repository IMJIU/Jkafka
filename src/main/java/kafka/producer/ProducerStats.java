package kafka.producer;

import com.google.common.collect.ImmutableMap;
import com.yammer.metrics.core.Meter;
import kafka.metrics.KafkaMetricsGroup;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author zhoulf
 * @create 2017-12-07 14:02
 **/
public class ProducerStats extends KafkaMetricsGroup {
    public String clientId;
    public Map<String, String> tags;
    public Meter serializationErrorRate;
    public Meter resendRate;
    public Meter failedSendRate;

    public ProducerStats(String clientId) {
        this.clientId = clientId;
        tags = ImmutableMap.of("clientId", clientId);
        serializationErrorRate = newMeter("SerializationErrorsPerSec", "errors", TimeUnit.SECONDS, tags);
        resendRate = newMeter("ResendsPerSec", "resends", TimeUnit.SECONDS, tags);
        failedSendRate = newMeter("FailedSendsPerSec", "failed sends", TimeUnit.SECONDS, tags);
    }
}
