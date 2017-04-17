package kafka.server;/**
 * Created by zhoulf on 2017/4/17.
 */

/**
 * @author
 * @create 2017-04-17 16:31
 **/
public enum BrokerStates {
    NotRunning((byte)0),
    Starting((byte)1),
    RecoveringFromUncleanShutdown((byte)2),
    RunningAsBroker((byte)3),
    RunningAsController((byte)4),
    PendingControlledShutdown((byte)6),
    BrokerShuttingDown((byte)7);

    public byte state;

    BrokerStates(byte state) {
        this.state = state;
    }
}
