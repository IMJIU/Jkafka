package kafka.server;

/**
 * @author zhoulf
 * @create 2017-10-30 17:36
 **/
public class ClientIdTopicPartition {
    String clientId;
    String topic;
    Integer partitionId;

    public ClientIdTopicPartition(String clientId, String topic, Integer partitionId) {
        this.clientId = clientId;
        this.topic = topic;
        this.partitionId = partitionId;
    }

    @Override
    public String toString() {
        return String.format("%s-%s-%d", clientId, topic, partitionId);
    }
}
