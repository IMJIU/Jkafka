package kafka.common;

/**
 * @author zhoulf
 * @create 2017-10-13 16:28
 **/
interface ClientIdBroker {
}

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
    public String toString() {
        return String.format("%s-%s-%d", clientId, brokerHost, brokerPort);
    }
}

        case

class ClientIdAllBrokers implements ClientIdBroker {
    public String clientId;

    public ClientIdAllBrokers(String clientId) {
        this.clientId = clientId;
    }

    @Override
    public String toString() {
        return String.format("%s-%s", clientId, "AllBrokers");
    }
}