package kafka.log;

import kafka.metrics.KafkaMetricsGroup;
import kafka.metrics.KafkaTimer;

import java.util.concurrent.TimeUnit;

/**
 * Created by Administrator on 2017/3/29.
 */
public class LogFlushStats extends KafkaMetricsGroup {
    public static KafkaTimer logFlushTimer = new KafkaTimer(newTimer("LogFlushRateAndTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS, null));
}
