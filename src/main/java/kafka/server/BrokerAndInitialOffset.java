package kafka.server;

import kafka.cluster.Broker;

public class BrokerAndInitialOffset {
    public Broker broker;
    public Long initOffset;

    public BrokerAndInitialOffset(Broker broker, Long initOffset) {
        this.broker = broker;
        this.initOffset = initOffset;
    }
}