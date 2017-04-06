package kafka.utils;

/**
 * Created by Administrator on 2017/4/4.
 */
public class ZKConfig {
    public VerifiableProperties props;
    /**
     * ZK host string
     */
    public String zkConnect ;

    /**
     * zookeeper session timeout
     */
    public Integer zkSessionTimeoutMs ;

    /**
     * the max time that the client waits to establish a connection to zookeeper
     */
    public Integer zkConnectionTimeoutMs;

    /**
     * how far a ZK follower can be behind a ZK leader
     */
    public Integer zkSyncTimeMs;

    public ZKConfig(VerifiableProperties props) {
        this.props = props;
        zkConnect = props.getString("zookeeper.connect");
        zkSessionTimeoutMs = props.getInt("zookeeper.session.timeout.ms", 6000);
        zkConnectionTimeoutMs = props.getInt("zookeeper.connection.timeout.ms", zkSessionTimeoutMs);
        zkSyncTimeMs = props.getInt("zookeeper.sync.time.ms", 2000);
    }


}
