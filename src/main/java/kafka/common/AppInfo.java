package kafka.common;

import com.yammer.metrics.core.Gauge;
import kafka.metrics.KafkaMetricsGroup;
import kafka.utils.Logging;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

/**
 * @author zhoulf
 * @create 2017-12-07 18 11
 **/

public class AppInfo  {
    private static  boolean isRegistered = false;
    private static final Logging log = Logging.getLogger(AppInfo.class.getName());
    private static final  Object lock = new Object();
    private static final KafkaMetricsGroup kafkaMetricsGroup = new KafkaMetricsGroup();
    public static void registerInfo() {
        synchronized (lock) {
            if (isRegistered) {
                return;
            }
        }

        try {
            Class clazz = AppInfo.class;
            String className = clazz.getSimpleName() + ".class";
            String classPath = clazz.getResource(className).toString();
            if (!classPath.startsWith("jar")) {
                // Class not from JAR;
                return;
            }
            String manifestPath = classPath.substring(0, classPath.lastIndexOf("!") + 1) + "/META-INF/MANIFEST.MF";

            Manifest mf = new Manifest();
            mf.read(new URL(manifestPath).openStream());
            String version = mf.getMainAttributes().get(new Attributes.Name("Version")).toString();

            kafkaMetricsGroup.newGauge("Version", new Gauge<String>() {
                public String value() {
                    return version;
                }
            });

            synchronized (lock) {
                isRegistered = true;
            }
        } catch (Throwable e) {
            log.warn(String.format("Can't read Kafka version from MANIFEST.MF. Possible cause: %s", e));
        }
    }
}

