package kafka.server;

import kafka.utils.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

/**
 * @author zhoulf
 * @create 2018-01-10 04 18
 **/

public class KafkaConfigTest {

    @Test
    public void testLogRetentionTimeHoursProvided() {
        Properties props = TestUtils.createBrokerConfig(0, 8181, null);
        props.put("log.retention.hours", "1");

        KafkaConfig cfg = new KafkaConfig(props);
        Assert.assertEquals(new Long(60L * 60L * 1000L), cfg.logRetentionTimeMillis);

    }

    @Test
    public void testLogRetentionTimeMinutesProvided() {
        Properties props = TestUtils.createBrokerConfig(0, 8181, null);
        props.put("log.retention.minutes", "30");

        KafkaConfig cfg = new KafkaConfig(props);
        Assert.assertEquals(new Long(30 * 60L * 1000L), cfg.logRetentionTimeMillis);

    }

    @Test
    public void testLogRetentionTimeMsProvided() {
        Properties props = TestUtils.createBrokerConfig(0, 8181, null);
        props.put("log.retention.ms", "1800000");

        KafkaConfig cfg = new KafkaConfig(props);
        Assert.assertEquals(new Long(30 * 60L * 1000L), cfg.logRetentionTimeMillis);

    }

    @Test
    public void testLogRetentionTimeNoConfigProvided() {
        Properties props = TestUtils.createBrokerConfig(0, 8181, null);

        KafkaConfig cfg = new KafkaConfig(props);
        Assert.assertEquals(new Long(24 * 7 * 60L * 60L * 1000L), cfg.logRetentionTimeMillis);

    }

    @Test
    public void testLogRetentionTimeBothMinutesAndHoursProvided() {
        Properties props = TestUtils.createBrokerConfig(0, 8181, null);
        props.put("log.retention.minutes", "30");
        props.put("log.retention.hours", "1");

        KafkaConfig cfg = new KafkaConfig(props);
        Assert.assertEquals(new Long(30 * 60L * 1000L), cfg.logRetentionTimeMillis);

    }

    @Test
    public void testLogRetentionTimeBothMinutesAndMsProvided() {
        Properties props = TestUtils.createBrokerConfig(0, 8181, null);
        props.put("log.retention.ms", "1800000");
        props.put("log.retention.minutes", "10");

        KafkaConfig cfg = new KafkaConfig(props);
        Assert.assertEquals(new Long(30 * 60L * 1000L), cfg.logRetentionTimeMillis);

    }

    @Test
    public void testAdvertiseDefaults() {
        Integer port = 9999;
        String hostName = "fake-host";

        Properties props = TestUtils.createBrokerConfig(0, port, null);
        props.put("host.name", hostName);

        KafkaConfig serverConfig = new KafkaConfig(props);

        Assert.assertEquals(serverConfig.advertisedHostName, hostName);
        Assert.assertEquals(serverConfig.advertisedPort, port);
    }

    @Test
    public void testAdvertiseConfigured() {
        Integer port = 9999;
        String advertisedHostName = "routable-host";
        Integer advertisedPort = 1234;

        Properties props = TestUtils.createBrokerConfig(0, port, null);
        props.put("advertised.host.name", advertisedHostName);
        props.put("advertised.port", advertisedPort.toString());

        KafkaConfig serverConfig = new KafkaConfig(props);

        Assert.assertEquals(serverConfig.advertisedHostName, advertisedHostName);
        Assert.assertEquals(serverConfig.advertisedPort, advertisedPort);
    }

    @Test
    public void testUncleanLeaderElectionDefault() {
        Properties props = TestUtils.createBrokerConfig(0, 8181, null);
        KafkaConfig serverConfig = new KafkaConfig(props);

        Assert.assertEquals(serverConfig.uncleanLeaderElectionEnable, true);
    }

    @Test
    public void testUncleanElectionDisabled() {
        Properties props = TestUtils.createBrokerConfig(0, 8181, null);
        props.put("unclean.leader.election.enable", String.valueOf(false));
        KafkaConfig serverConfig = new KafkaConfig(props);

        Assert.assertEquals(serverConfig.uncleanLeaderElectionEnable, false);
    }

    @Test
    public void testUncleanElectionEnabled() {
        Properties props = TestUtils.createBrokerConfig(0, 8181, null);
        props.put("unclean.leader.election.enable", String.valueOf(true));
        KafkaConfig serverConfig = new KafkaConfig(props);

        Assert.assertEquals(serverConfig.uncleanLeaderElectionEnable, true);
    }

    @Test
    public void testUncleanElectionInvalid() {
        Properties props = TestUtils.createBrokerConfig(0, 8181, null);
        props.put("unclean.leader.election.enable", "invalid");

//        intercept<IllegalArgumentException> {
//            new KafkaConfig(props);
//        }
    }

    @Test
    public void testLogRollTimeMsProvided() {
        Properties props = TestUtils.createBrokerConfig(0, 8181, null);
        props.put("log.roll.ms", "1800000");

        KafkaConfig cfg = new KafkaConfig(props);
        Assert.assertEquals(new Long(30 * 60L * 1000L), cfg.logRollTimeMillis);

    }

    @Test
    public void testLogRollTimeBothMsAndHoursProvided() {
        Properties props = TestUtils.createBrokerConfig(0, 8181, null);
        props.put("log.roll.ms", "1800000");
        props.put("log.roll.hours", "1");

        KafkaConfig cfg = new KafkaConfig(props);
        Assert.assertEquals(new Long(30 * 60L * 1000L), cfg.logRollTimeMillis);

    }

    @Test
    public void testLogRollTimeNoConfigProvided() {
        Properties props = TestUtils.createBrokerConfig(0, 8181, null);

        KafkaConfig cfg = new KafkaConfig(props);
        Assert.assertEquals(new Long(24 * 7 * 60L * 60L * 1000L), cfg.logRollTimeMillis);

    }


}
