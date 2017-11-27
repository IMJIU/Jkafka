package kafka.server;

/**
 * @author zhoulf
 * @create 2017-11-27 12 18
 **/

import kafka.utils.Logging;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.Watcher;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * This class registers the broker in zookeeper to allow
 * other brokers and consumers to detect failures. It uses an ephemeral znode with the path:
 * /brokers/<0...N> --> advertisedPort advertisedHost
 * <p>
 * Right now our definition of health is fairly naive. If we register in zk we are healthy, otherwise
 * we are dead.
 */
class KafkaHealthcheck extends Logging {
    private Integer brokerId;
    private String advertisedHost;
    private Integer advertisedPort;
    private Integer zkSessionTimeoutMs;
    private ZkClient zkClient;

    public KafkaHealthcheck(Integer brokerId, String advertisedHost, Integer advertisedPort, Integer zkSessionTimeoutMs, ZkClient zkClient) {
        this.brokerId = brokerId;
        this.advertisedHost = advertisedHost;
        this.advertisedPort = advertisedPort;
        this.zkSessionTimeoutMs = zkSessionTimeoutMs;
        this.zkClient = zkClient;
    }

    public String brokerIdPath = ZkUtils.BrokerIdsPath + "/" + brokerId;
    public SessionExpireListener sessionExpireListener = new SessionExpireListener();

    public void startup() {
        zkClient.subscribeStateChanges(sessionExpireListener);
        register();
    }

    /**
     * Register this broker as "alive" in zookeeper
     */
    public void register() {
        try {
            String advertisedHostName;
            if (advertisedHost == null || advertisedHost.trim().isEmpty())
                advertisedHostName = InetAddress.getLocalHost().getCanonicalHostName();
            else
                advertisedHostName = advertisedHost;
            Integer jmxPort = Integer.parseInt(System.getProperty("com.sun.management.jmxremote.port", "-1"));
            ZkUtils.registerBrokerInZk(zkClient, brokerId, advertisedHostName, advertisedPort, zkSessionTimeoutMs, jmxPort);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    /**
     * When we get a SessionExpired event, we lost all ephemeral nodes and zkclient has reestablished a
     * connection for us. We need to re-register this broker in the broker registry.
     */
    class SessionExpireListener implements IZkStateListener {
        //        @throws(classOf<Exception>)
        public void handleStateChanged(Watcher.Event.KeeperState state) {
            // do nothing, since zkclient will do reconnect for us.;
        }

        /**
         * Called after the zookeeper session has expired and a new session has been created. You would have to re-create
         * any ephemeral nodes here.
         *
         * @throws Exception On any error.
         */
//        @throws(classOf<Exception>)
        public void handleNewSession() {
            info("re-registering broker info in ZK for broker " + brokerId);
            register();
            info("done re-registering broker");
            info(String.format("Subscribing to %s path to watch for new topics", ZkUtils.BrokerTopicsPath));
        }
    }

}
