package kafka.common;

/**
 * @author zhoulf
 * @create 2017-10-30 17:37
 **/
public class ClientIdAndTopic implements ClientIdTopic {
    public String clientId;
    public String topic;

    public ClientIdAndTopic(String clientId, String topic) {
        this.clientId = clientId;
        this.topic = topic;
    }

    @Override
    public String toString() {
        return "%s-%s".format(clientId, topic);
    }

}
