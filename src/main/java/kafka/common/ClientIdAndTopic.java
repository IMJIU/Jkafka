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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ClientIdAndTopic that = (ClientIdAndTopic) o;

        if (clientId != null ? !clientId.equals(that.clientId) : that.clientId != null) return false;
        return topic != null ? topic.equals(that.topic) : that.topic == null;

    }

    @Override
    public int hashCode() {
        int result = clientId != null ? clientId.hashCode() : 0;
        result = 31 * result + (topic != null ? topic.hashCode() : 0);
        return result;
    }
}
