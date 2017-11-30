package kafka.metrics;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.*;
import com.yammer.metrics.core.Timer;
import kafka.utils.Logging;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by Administrator on 2017/3/29.
 */
public class KafkaMetricsGroup extends Logging {

    /**
     * Creates a new MetricName object for gauges, meters, etc. created for this
     * metrics group.
     *
     * @param name Descriptive name of the metric.
     * @param tags Additional attributes which mBean will have.
     * @return Sanitized metric name object.
     */
    private static MetricName metricName(String name, Map<String, String> tags) {
        Class klass = KafkaMetricsGroup.class;
        String pkg = (klass.getPackage() == null) ? "" : klass.getPackage().getName();
        String simpleName = klass.getSimpleName().replaceAll("\\$$", "");
        return explicitMetricName(pkg, simpleName, name, tags);
    }

    private static MetricName explicitMetricName(String group, String typeName, String name, Map<String, String> tags) {
        if (tags == null) {
            tags = Maps.newHashMap();
        }
        StringBuilder nameBuilder = new StringBuilder();
        nameBuilder.append(group);
        nameBuilder.append(":type=");
        nameBuilder.append(typeName);
        if (name.length() > 0) {
            nameBuilder.append(",name=");
            nameBuilder.append(name);
        }

        String scope = KafkaMetricsGroup.toScope(tags).orElse(null);
        Optional<String> tagsName = KafkaMetricsGroup.toMBeanName(tags);
        if (tagsName.isPresent()) {
            nameBuilder.append(",").append(tagsName.get());
        }
        return new MetricName(group, typeName, name, scope, nameBuilder.toString());
    }

    public <T> Gauge<T> newGauge(String name, Gauge<T> metric) {
        return Metrics.defaultRegistry().newGauge(metricName(name, Collections.EMPTY_MAP), metric);
    }

    public <T> Gauge<T> newGauge(String name, Gauge<T> metric, Map<String, String> tags) {
        return Metrics.defaultRegistry().newGauge(metricName(name, tags), metric);
    }

    public static Meter newMeter(String name, String eventType, TimeUnit timeUnit) {
        return Metrics.defaultRegistry().newMeter(metricName(name, Maps.newHashMap()), eventType, timeUnit);
    }

    public  static Meter newMeter(String name, String eventType, TimeUnit timeUnit, Map<String, String> tags) {
        return Metrics.defaultRegistry().newMeter(metricName(name, tags), eventType, timeUnit);
    }


    public Histogram newHistogram(String name, Boolean biased, Map<String, String> tags) {
        if (biased == null) {
            biased = true;
        }
        return Metrics.defaultRegistry().newHistogram(metricName(name, tags), biased);
    }
    public static Timer newTimer(String name, TimeUnit durationUnit, TimeUnit rateUnit){
        return newTimer(name,durationUnit,rateUnit, ImmutableMap.of());
    }
    public static Timer newTimer(String name, TimeUnit durationUnit, TimeUnit rateUnit, Map<String, String> tags) {
        return Metrics.defaultRegistry().newTimer(metricName(name, tags), durationUnit, rateUnit);
    }

    //
//    def removeMetric(String name, scala tags.collection.Map[String, String]=Map.empty);
//
//    =
//            Metrics.defaultRegistry().;
//
//    removeMetric(metricName(name, tags);
//
//    );
//
//
//}
//
//    object KafkaMetricsGroup extends KafkaMetricsGroup with Logging{
///**
// * To make sure all the metrics be de-registered after consumer/producer close, the metric names should be
// * put into the metric name set.
// */
//private Integer immutable consumerMetricNameList.List[MetricName]=immutable.List[MetricName](
//        // kafka.consumer.ZookeeperConsumerConnector;
//        new MetricName("kafka.consumer","ZookeeperConsumerConnector","FetchQueueSize"),
//        new MetricName("kafka.consumer","ZookeeperConsumerConnector","KafkaCommitsPerSec"),
//        new MetricName("kafka.consumer","ZookeeperConsumerConnector","ZooKeeperCommitsPerSec"),
//        new MetricName("kafka.consumer","ZookeeperConsumerConnector","RebalanceRateAndTime"),
//        new MetricName("kafka.consumer","ZookeeperConsumerConnector","OwnedPartitionsCount"),
//
//        // kafka.consumer.ConsumerFetcherManager;
//        new MetricName("kafka.consumer","ConsumerFetcherManager","MaxLag"),
//        new MetricName("kafka.consumer","ConsumerFetcherManager","MinFetchRate"),
//
//        // kafka.server.AbstractFetcherThread <-- kafka.consumer.ConsumerFetcherThread;
//        new MetricName("kafka.server","FetcherLagMetrics","ConsumerLag"),
//
//        // kafka.consumer.ConsumerTopicStats <-- kafka.consumer.{ConsumerIterator, PartitionTopicInfo}
//        new MetricName("kafka.consumer","ConsumerTopicMetrics","MessagesPerSec"),
//
//        // kafka.consumer.ConsumerTopicStats;
//        new MetricName("kafka.consumer","ConsumerTopicMetrics","BytesPerSec"),
//
//        // kafka.server.AbstractFetcherThread <-- kafka.consumer.ConsumerFetcherThread;
//        new MetricName("kafka.server","FetcherStats","BytesPerSec"),
//        new MetricName("kafka.server","FetcherStats","RequestsPerSec"),
//
//        // kafka.consumer.FetchRequestAndResponseStats <-- kafka.consumer.SimpleConsumer;
//        new MetricName("kafka.consumer","FetchRequestAndResponseMetrics","FetchResponseSize"),
//        new MetricName("kafka.consumer","FetchRequestAndResponseMetrics","FetchRequestRateAndTimeMs"),
//
//        /**
//         * ProducerRequestStats <-- SyncProducer
//         * metric for SyncProducer in fetchTopicMetaData() needs to be removed when consumer is closed.
//         */
//        new MetricName("kafka.producer","ProducerRequestMetrics","ProducerRequestRateAndTimeMs"),
//        new MetricName("kafka.producer","ProducerRequestMetrics","ProducerRequestSize");
//        );
//
//private Integer immutable producerMetricNameList.List[MetricName]=immutable.List[MetricName](
//        // kafka.producer.ProducerStats <-- DefaultEventHandler <-- Producer;
//        new MetricName("kafka.producer","ProducerStats","SerializationErrorsPerSec"),
//        new MetricName("kafka.producer","ProducerStats","ResendsPerSec"),
//        new MetricName("kafka.producer","ProducerStats","FailedSendsPerSec"),
//
//        // kafka.producer.ProducerSendThread;
//        new MetricName("kafka.producer.async","ProducerSendThread","ProducerQueueSize"),
//
//        // kafka.producer.ProducerTopicStats <-- kafka.producer.{Producer, async.DefaultEventHandler}
//        new MetricName("kafka.producer","ProducerTopicMetrics","MessagesPerSec"),
//        new MetricName("kafka.producer","ProducerTopicMetrics","DroppedMessagesPerSec"),
//        new MetricName("kafka.producer","ProducerTopicMetrics","BytesPerSec"),
//
//        // kafka.producer.ProducerRequestStats <-- SyncProducer;
//        new MetricName("kafka.producer","ProducerRequestMetrics","ProducerRequestRateAndTimeMs"),
//        new MetricName("kafka.producer","ProducerRequestMetrics","ProducerRequestSize");
//        );
//
    private static Optional<String> toMBeanName(Map<String, String> tags) {
        Map<String, String> filteredTags = tags;
        Iterator<Map.Entry<String, String>> it = filteredTags.entrySet().iterator();
        StringBuilder sb = new StringBuilder();
        while (it.hasNext()) {
            Map.Entry<String, String> entry = it.next();
            if (entry.getValue() == "") {
                it.remove();
            } else {
                sb.append(String.format("%s=%s", entry.getKey(), entry.getValue())).append(",");
            }
        }
        if (filteredTags.size() > 0) {
            if (sb.charAt(sb.length() - 1) == ',') {
                return Optional.of(sb.deleteCharAt(sb.length() - 1).toString());
            }
            return Optional.of(sb.toString());
        } else {
            return Optional.empty();
        }
    }

    private static Optional<String> toScope(Map<String, String> tags) {
        Map<String, String> filteredTags = tags;
        TreeMap treeMap = new TreeMap();
        treeMap.putAll(filteredTags);

        StringBuilder sb = new StringBuilder();
        Iterator<Map.Entry<String, String>> it = treeMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, String> entry = it.next();
            if (entry.getValue() == "") {
                it.remove();
            } else {
                sb.append(String.format("%s.%s", entry.getKey(), entry.getValue().replaceAll("\\.", "_"))).append(".");
            }
        }
        if (filteredTags.size() > 0) {
            return Optional.of(sb.toString());
        } else {
            return Optional.empty();
        }
    }
//
//        def removeAllConsumerMetrics(String clientId){
//        FetchRequestAndResponseStatsRegistry.removeConsumerFetchRequestAndResponseStats(clientId);
//        ConsumerTopicStatsRegistry.removeConsumerTopicStat(clientId);
//        ProducerRequestStatsRegistry.removeProducerRequestStats(clientId);
//        removeAllMetricsInList(KafkaMetricsGroup.consumerMetricNameList,clientId);
//        }
//
        def removeAllProducerMetrics(String clientId){
        ProducerRequestStatsRegistry.removeProducerRequestStats(clientId);
        ProducerTopicStatsRegistry.removeProducerTopicStats(clientId);
        ProducerStatsRegistry.removeProducerStats(clientId);
        removeAllMetricsInList(KafkaMetricsGroup.producerMetricNameList,clientId);
        }
//
//private def removeAllMetricsInList(immutable metricNameList.List[MetricName],String clientId){
//        metricNameList.foreach(metric=>{
//        Integer pattern=(".*clientId="+clientId+".*").r;
//        Integer registeredMetrics=scala.collection.JavaConversions.asScalaSet(Metrics.defaultRegistry().allMetrics().keySet());
//        for(registeredMetric<-registeredMetrics){
//        if(registeredMetric.getGroup==metric.getGroup&&;
//        registeredMetric.getName==metric.getName&&;
//        registeredMetric.getType==metric.getType){
//        pattern.findFirstIn(registeredMetric.getMBeanName)match{
//        case Some(_)=>{
//        Integer beforeRemovalSize=Metrics.defaultRegistry().allMetrics().keySet().size;
//        Metrics.defaultRegistry().removeMetric(registeredMetric);
//        Integer afterRemovalSize=Metrics.defaultRegistry().allMetrics().keySet().size;
//        trace(String.format("Removing metric %s. Metrics registry size reduced from %d to %d",
//        registeredMetric,beforeRemovalSize,afterRemovalSize));
//        }
//        case _=>
//        }
//        }
//        }
//        });
//        }
}
