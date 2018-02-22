package kafka.server;

import kafka.cluster.Broker;

public class BrokerAndInitialOffset {
    public Broker broker;
    public Long initOffset;

    public BrokerAndInitialOffset(Broker broker, Long initOffset) {
        this.broker = broker;
        this.initOffset = initOffset;
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BrokerAndInitialOffset that = (BrokerAndInitialOffset) o;

        if (broker != null ? !broker.equals(that.broker) : that.broker != null) return false;
        return initOffset != null ? initOffset.equals(that.initOffset) : that.initOffset == null;
    }

    @Override
    public int hashCode() {
        int result = broker != null ? broker.hashCode() : 0;
        result = 31 * result + (initOffset != null ? initOffset.hashCode() : 0);
        return result;
    }
}