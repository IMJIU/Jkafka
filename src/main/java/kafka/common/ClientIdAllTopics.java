package kafka.common;

/**
 * @author zhoulf
 * @create 2017-10-30 17:37
 **/
public class ClientIdAllTopics implements ClientIdTopic {
    public String clientId;

    public ClientIdAllTopics(String clientId) {
        this.clientId = clientId;
    }

    @Override
    public String toString() {
        return "%s-%s".format(clientId, "AllTopics");
    }

}
