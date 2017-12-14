package kafka.metric;

import com.google.common.collect.Lists;
import com.yammer.metrics.Metrics;
import kafka.consumer.ConsumerConfig;
import kafka.integration.KafkaServerTestHarness;
import kafka.server.KafkaConfig;
import kafka.utils.TestUtils;
import kafka.utils.TestZKUtils;
import org.junit.Assert;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import static kafka.utils.TestUtils.*;
import static kafka.message.CompressionCodec.*;
public class MetricsTest extends KafkaServerTestHarness {
    String zookeeperConnect = TestZKUtils.zookeeperConnect;
    int numNodes = 2;
    int numParts = 2;
    String topic = "topic1";
    List<KafkaConfig> configs = Lists.newArrayList();

    int nMessages = 2;

    public MetricsTest() {
        try {
            for (Properties props : TestUtils.createBrokerConfigs(numNodes, true)) {
                KafkaConfig config = new KafkaConfig(props);
                config.zkConnect = zookeeperConnect;
                config.numPartitions = numParts;
                configs.add(config);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Before
    public void before(){
        super.setUp();
    }

    @Override
    public void tearDown() {
        super.tearDown();
    }

    public void testMetricsLeak() {
        // create topic topic1 with 1 partition on broker 0;
        createTopic(zkClient, topic, 1, 1, servers, null);
        // force creation not client's specific metrics.;
        createAndShutdownStep("group0", "consumer0", "producer0");

        int countOfStaticMetrics = Metrics.defaultRegistry().allMetrics().keySet().size();

        for (int i=0;i<5;i++){
            createAndShutdownStep("group" + i % 3, "consumer" + i % 2, "producer" + i % 2);
            Assert.assertEquals(countOfStaticMetrics, Metrics.defaultRegistry().allMetrics().keySet().size());
        }
    }

    public void createAndShutdownStep(String group, String consumerId, String producerId) {
        List<String> sentMessages1 = TestUtils.sendMessages(configs, topic, producerId, nMessages, "batch1", NoCompressionCodec, 1);
        // create a consumer;
        ConsumerConfig consumerConfig1 = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnect, group, consumerId,-1L));
        val zkConsumerConnector1 = new ZookeeperConsumerConnector(consumerConfig1, true);
        val topicMessageStreams1 = zkConsumerConnector1.createMessageStreams(Map(topic -> 1), new StringDecoder(), new StringDecoder());
        val receivedMessages1 = getMessages(nMessages, topicMessageStreams1);

        zkConsumerConnector1.shutdown();
    }
}
