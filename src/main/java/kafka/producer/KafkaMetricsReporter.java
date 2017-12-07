package kafka.producer;

import kafka.producer.async.KafkaMetricsConfig;
import kafka.utils.Utils;
import kafka.utils.VerifiableProperties;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author zhoulf
 * @create 2017-12-01 14:24
 **/
public interface KafkaMetricsReporter {
    AtomicBoolean ReporterStarted = new AtomicBoolean(false);

    void init(VerifiableProperties props);


     static void startReporters(VerifiableProperties verifiableProps) {
        synchronized (ReporterStarted) {
            if (ReporterStarted.get() == false) {
                KafkaMetricsConfig metricsConfig = new KafkaMetricsConfig(verifiableProps);
                if (metricsConfig.reporters.size() > 0) {
                    metricsConfig.reporters.forEach(reporterType -> {
                        KafkaMetricsReporter reporter = Utils.createObject(reporterType);
                        reporter.init(verifiableProps);
                        if (reporter instanceof KafkaMetricsReporterMBean)
                            Utils.registerMBean(reporter, ((KafkaMetricsReporterMBean) reporter).getMBeanName());
                    });
                    ReporterStarted.set(true);
                }
            }
        }
    }
}

interface KafkaMetricsReporterMBean {
    void startReporter(Long pollingPeriodInSeconds);

    void stopReporter();

    /**
     * @return The name with which the MBean will be registered.
     */
    String getMBeanName();
}
