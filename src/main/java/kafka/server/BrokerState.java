package kafka.server;/**
 * Created by zhoulf on 2017/4/17.
 */

/**
 * @author
 * @create 2017-04-17 16:31
 **/
public class BrokerState {
    public volatile byte currentState = BrokerStates.NotRunning.state;

    public void newState(BrokerStates newState) {
        this.newState(newState.state);
    }

    // Allowing undefined custom state
    public void newState(byte newState) {
        currentState = newState;
    }
}
