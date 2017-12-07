package kafka.producer;

/**
 * @author zhoulf
 * @create 2017-12-07 14:03
 **/

import kafka.common.ClientIdAllTopics;
import kafka.common.ClientIdAndTopic;
import kafka.common.ClientIdTopic;
import kafka.func.Handler;
import kafka.utils.Pool;

import java.util.Optional;

/**
 * Tracks metrics for each topic the given producer client has produced data to.
 *
 * param clientId The clientId of the given producer client.
 */
public class ProducerTopicStats {
    public String clientId;

    public ProducerTopicStats(String clientId) {
        this.clientId = clientId;
    }

    private Handler<ClientIdTopic, ProducerTopicMetrics> valueFactory = (ClientIdTopic k) -> new ProducerTopicMetrics(k);
    private Pool<ClientIdTopic, ProducerTopicMetrics> stats = new Pool<ClientIdTopic, ProducerTopicMetrics>(Optional.of(valueFactory));
    private ProducerTopicMetrics allTopicsStats = new ProducerTopicMetrics(new ClientIdAllTopics(clientId)); // to differentiate from a topic named AllTopics;

    public ProducerTopicMetrics getProducerAllTopicsStats() {
        return allTopicsStats;
    }

    public ProducerTopicMetrics getProducerTopicStats(String topic) {
        return stats.getAndMaybePut(new ClientIdAndTopic(clientId, topic));
    }
}