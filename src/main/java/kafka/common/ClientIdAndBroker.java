package kafka.common;

/**
 * @author zhoulf
 * @create 2017-10-13 16:28
 **/


public class ClientIdAndBroker implements ClientIdBroker {
    public String clientId;
    public String brokerHost;
    public Integer brokerPort;

    public ClientIdAndBroker(String clientId, String brokerHost, Integer brokerPort) {
        this.clientId = clientId;
        this.brokerHost = brokerHost;
        this.brokerPort = brokerPort;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ClientIdAndBroker that = (ClientIdAndBroker) o;

        if (clientId != null ? !clientId.equals(that.clientId) : that.clientId != null) return false;
        if (brokerHost != null ? !brokerHost.equals(that.brokerHost) : that.brokerHost != null) return false;
        return brokerPort != null ? brokerPort.equals(that.brokerPort) : that.brokerPort == null;

    }

    @Override
    public int hashCode() {
        int result = clientId != null ? clientId.hashCode() : 0;
        result = 31 * result + (brokerHost != null ? brokerHost.hashCode() : 0);
        result = 31 * result + (brokerPort != null ? brokerPort.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return String.format("%s-%s-%d", clientId, brokerHost, brokerPort);
    }
}

