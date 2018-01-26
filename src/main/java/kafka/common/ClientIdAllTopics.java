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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ClientIdAllTopics that = (ClientIdAllTopics) o;

        return clientId != null ? clientId.equals(that.clientId) : that.clientId == null;

    }

    @Override
    public int hashCode() {
        return clientId != null ? clientId.hashCode() : 0;
    }
}
