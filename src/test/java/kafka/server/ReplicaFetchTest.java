package kafka.server;

import com.google.common.collect.Lists;
import kafka.func.Fun;
import kafka.log.TopicAndPartition;
import kafka.producer.DefaultPartitioner;
import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.serializer.StringEncoder;
import kafka.utils.Sc;
import kafka.utils.TestUtils;
import kafka.zk.ZooKeeperTestHarness;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import static kafka.utils.TestUtils.*;

/**
 * @author zhoulf
 * @create 2018-01-10 15 18
 **/


public class ReplicaFetchTest extends ZooKeeperTestHarness {
    List<Properties> props = createBrokerConfigs(2, false);
    List<KafkaConfig> configs = Sc.map(props, p -> new KafkaConfig(p));
    List<KafkaServer> brokers = null;
    String topic1 = "foo";
    String topic2 = "bar";

    @Override
    public void setUp() throws IOException, InterruptedException {
        super.setUp();
        brokers = Sc.map(configs, config -> TestUtils.createServer(config));
    }

    @Override
    public void tearDown() {
        brokers.forEach(b -> b.shutdown());
        super.tearDown();
    }

    @Test
    public void testReplicaFetcherThread() {
        Integer partition = 0;
        List<String> testMessageList1 = Lists.newArrayList("test1", "test2", "test3", "test4");
        List<String> testMessageList2 = Lists.newArrayList("test5", "test6", "test7", "test8");

        // create a topic and partition and await leadership;
        for (String topic : Lists.newArrayList(topic1, topic2)) {
            createTopic(zkClient, topic, 1, 2, brokers, null);
        }

        // send test messages to leader;
        Producer<String, String> producer = TestUtils.createProducer(TestUtils.getBrokerListStrFromConfigs(configs),
                StringEncoder.class.getName(),
                StringEncoder.class.getName(), DefaultPartitioner.class.getName(), null);
        List<KeyedMessage<String, String>> messages = Sc.map(testMessageList1, m -> new KeyedMessage(topic1, m, m));
        messages.addAll(Sc.map(testMessageList2, (m -> new KeyedMessage(topic2, m, m))));
        producer.send(messages);
        producer.close();

        Fun<Boolean> logsMatch = () -> {
            boolean result = true;
            for (String topic : Lists.newArrayList(topic1, topic2)) {
                TopicAndPartition topicAndPart = new TopicAndPartition(topic, partition);
                Long expectedOffset = brokers.get(0).getLogManager().getLog(topicAndPart).get().logEndOffset();
                if (result && expectedOffset > 0) {
                    boolean b = true;
                    for (KafkaServer item : brokers) {
                        b = b && (expectedOffset == item.getLogManager().getLog(topicAndPart).get().logEndOffset());
                    }
                    result = b;
                }else{
                    result = false;
                }
            }
            return result;
        };
        waitUntilTrue(logsMatch, "Broker logs should be identical");
    }


}
