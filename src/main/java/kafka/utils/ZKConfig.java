package kafka.utils;

/**
 * Created by Administrator on 2017/4/4.
 */
public class ZKConfig {
    public VerifiableProperties props;

    public ZKConfig(VerifiableProperties props) {
        this.props = props;
    }

    /**
     * ZK host string
     */
    public String zkConnect = props.getString("zookeeper.connect");

    /**
     * zookeeper session timeout
     */
    public Integer zkSessionTimeoutMs = props.getInt("zookeeper.session.timeout.ms", 6000);

    /**
     * the max time that the client waits to establish a connection to zookeeper
     */
    public Integer zkConnectionTimeoutMs = props.getInt("zookeeper.connection.timeout.ms", zkSessionTimeoutMs);

    /**
     * how far a ZK follower can be behind a ZK leader
     */
    public Integer zkSyncTimeMs = props.getInt("zookeeper.sync.time.ms", 2000);
}
