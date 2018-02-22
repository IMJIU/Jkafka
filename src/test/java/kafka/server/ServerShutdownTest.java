package kafka.server;

import com.google.common.collect.Lists;
import kafka.api.FetchResponse;
import kafka.consumer.SimpleConsumer;
import kafka.message.ByteBufferMessageSet;
import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.serializer.StringEncoder;
import kafka.utils.*;
import kafka.zk.ZooKeeperTestHarness;
import org.I0Itec.zkclient.exception.ZkException;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.Properties;

public class ServerShutdownTest extends ZooKeeperTestHarness {
    Integer port = TestUtils.choosePort();
    Properties props = TestUtils.createBrokerConfig(0, port, null);
    KafkaConfig config = new KafkaConfig(props);

    String host = "localhost";
    String topic = "test";
    List<String> sent1 = Lists.newArrayList("hello", "there");
    List<String> sent2 = Lists.newArrayList("more", "messages");

    @Test
    public void testCleanShutdown() {
        KafkaServer server = new KafkaServer(config, Time.get());
        server.startup();
        Producer<Integer, String> producer = TestUtils.createProducer(TestUtils.getBrokerListStrFromConfigs(Lists.newArrayList(config)),
                StringEncoder.class.getName(),
                IntEncoder.class.getName(), null, null);

        // create topic;
        TestUtils.createTopic(zkClient, topic, 1, 1, Lists.newArrayList(server), null);

        // send some messages;
        producer.send(Sc.map(sent1, m -> new KeyedMessage<>(topic, 0, m)));

        // do a clean shutdown and check that offset checkpoint file exists;
        server.shutdown();
        for (String logDir : config.logDirs) {
            File OffsetCheckpointFile = new File(logDir, server.logManager.RecoveryPointCheckpointFile);
            Assert.assertTrue(OffsetCheckpointFile.exists());
            Assert.assertTrue(OffsetCheckpointFile.length() > 0);
        }
        producer.close();
    /* now restart the server and check that the written data is still readable and everything still works */
        server = new KafkaServer(config, Time.get());
        server.startup();

        // wait for the broker to receive the update metadata request after startup;
        TestUtils.waitUntilMetadataIsPropagated(Lists.newArrayList(server), topic, 0);

        producer = TestUtils.createProducer(TestUtils.getBrokerListStrFromConfigs(Lists.newArrayList(config)),
                StringEncoder.class.getName(),
                IntEncoder.class.getName(), null, null);
        SimpleConsumer consumer = new SimpleConsumer(host, port, 1000000, 64 * 1024, "");

        ByteBufferMessageSet fetchedMessage = null;
        while (fetchedMessage == null || fetchedMessage.validBytes() == 0) {
            FetchResponse fetched = consumer.fetch(new FetchRequestBuilder().addFetch(topic, 0, 0L, 10000).maxWait(0).build());
            fetchedMessage = fetched.messageSet(topic, 0);
        }
        Assert.assertEquals(sent1, Sc.map(fetchedMessage, m -> Utils.readString(m.message.payload())));
        Long newOffset = fetchedMessage.last().get().nextOffset();

        // send some more messages;
        producer.send(Sc.map(sent2, m -> new KeyedMessage<>(topic, 0, m)));

        fetchedMessage = null;
        while (fetchedMessage == null || fetchedMessage.validBytes() == 0) {
            FetchResponse fetched = consumer.fetch(new FetchRequestBuilder().addFetch(topic, 0, newOffset, 10000).build());
            fetchedMessage = fetched.messageSet(topic, 0);
        }
        Assert.assertEquals(sent2, Sc.map(fetchedMessage, m -> Utils.readString(m.message.payload())));

        consumer.close();
        producer.close();
        server.shutdown();
        Utils.rm(server.config.logDirs);
        verifyNonDaemonThreadsStatus();
    }

    @Test
    public void testCleanShutdownWithDeleteTopicEnabled() {
        Properties newProps = TestUtils.createBrokerConfig(0, port, null);
        newProps.setProperty("delete.topic.enable", "true");
        KafkaConfig newConfig = new KafkaConfig(newProps);
        KafkaServer server = new KafkaServer(newConfig, Time.get());
        server.startup();
        server.shutdown();
        server.awaitShutdown();
        Utils.rm(server.config.logDirs);
        verifyNonDaemonThreadsStatus();
    }

    @Test
    public void testCleanShutdownAfterFailedStartup() {
        Properties newProps = TestUtils.createBrokerConfig(0, port, null);
        newProps.setProperty("zookeeper.connect", "65535 fakehostthatwontresolve");
        KafkaConfig newConfig = new KafkaConfig(newProps);
        KafkaServer server = new KafkaServer(newConfig, Time.get());
        try {
            server.startup();
            Assert.fail("Expected KafkaServer setup to fail, throw exception");
        } catch (ZkException e) {
            // Try to clean up carefully without hanging even if the test fails. This means trying to accurately;
            // identify the correct exception, making sure the server was shutdown, and cleaning up if anything;
            // goes wrong so that awaitShutdown doesn't hang;
            Assert.assertEquals(server.brokerState.currentState, BrokerStates.NotRunning.state);
            if (server.brokerState.currentState != BrokerStates.NotRunning.state)
                server.shutdown();
        } catch (Throwable e) {
            Assert.fail("Expected KafkaServer setup to fail with connection exception but caught a different exception.");
            server.shutdown();
        }
        server.awaitShutdown();
        Utils.rm(server.config.logDirs);
        verifyNonDaemonThreadsStatus();
    }

    public void verifyNonDaemonThreadsStatus() {
        Assert.assertEquals(0,
                Sc.count(Thread.getAllStackTraces().keySet(),
                        t -> !t.isDaemon() && t.isAlive() && t.getClass().getCanonicalName().toLowerCase().startsWith("kafka")));
    }
}