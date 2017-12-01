package kafka.producer.async;

import kafka.utils.Utils;
import kafka.utils.VerifiableProperties;

import java.util.List;

/**
 * @author zhoulf
 * @create 2017-12-01 14:26
 **/
public class KafkaMetricsConfig {
    VerifiableProperties props;

    public KafkaMetricsConfig(VerifiableProperties props) {
        this.props = props;
    }

    /**
     * Comma-separated list of reporter types. These classes should be on the
     * classpath and will be instantiated at run-time.
     */
    public List<String> reporters = Utils.parseCsvList(props.getString("kafka.metrics.reporters", ""));

    /**
     * The metrics polling interval (in seconds).
     */
    public int pollingIntervalSecs = props.getInt("kafka.metrics.polling.interval.secs", 10);
}

