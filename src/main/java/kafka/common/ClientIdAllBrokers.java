package kafka.common;

/**
 * @author zhoulf
 * @create 2017-10-27 17:05
 **/

public class ClientIdAllBrokers implements ClientIdBroker {
    public String clientId;

    public ClientIdAllBrokers(String clientId) {
        this.clientId = clientId;
    }

    @Override
    public String toString() {
        return String.format("%s-%s", clientId, "AllBrokers");
    }

}