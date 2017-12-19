package kafka.consumer;

import kafka.utils.Logging;

public class Consumer extends Logging {
    /**
     * Create a ConsumerConnector
     *
     * @param config at the minimum, need to specify the groupid of the consumer and the zookeeper
     *               connection string zookeeper.connect.
     */
    public ConsumerConnector create(ConsumerConfig config) {
        ConsumerConnector consumerConnect = new ZookeeperConsumerConnector(config);
        return consumerConnect;
    }

    /**
     * Create a ConsumerConnector
     *
     * @param config at the minimum, need to specify the groupid of the consumer and the zookeeper
     *               connection string zookeeper.connect.
     */
    public kafka.javaapi.consumer.ConsumerConnector createJavaConsumerConnector(ConsumerConfig config) {
        kafka.javaapi.consumer.ConsumerConnector consumerConnect = new kafka.javaapi.consumer.ZookeeperConsumerConnector(config);
        return consumerConnect;
    }
}