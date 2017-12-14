package kafka.integration;

/**
 * @author zhoulf
 * @create 2017-11-27 51 17
 **/

import kafka.common.KafkaException;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.Sc;
import kafka.utils.TestUtils;
import kafka.utils.Utils;
import kafka.zk.ZooKeeperTestHarness;
import org.junit.After;
import org.junit.Before;

import java.util.List;

/**
 * A test harness that brings up some number of broker nodes
 */
public abstract class KafkaServerTestHarness extends ZooKeeperTestHarness {

    public List<KafkaConfig> configs;
    public List<KafkaServer> servers = null;
    public String brokerList = null;

    @Before
    public void setUp() {
        if (configs.size() <= 0)
            throw new KafkaException("Must suply at least one server config.");
        brokerList = TestUtils.getBrokerListStrFromConfigs(configs);
        servers = Sc.map(configs, c -> TestUtils.createServer(c, null));
    }

    @After
    public void tearDown() {
        servers.forEach(server -> server.shutdown());
        servers.forEach(server -> server.config.logDirs.forEach(d -> Utils.rm(d)));
    }
}
