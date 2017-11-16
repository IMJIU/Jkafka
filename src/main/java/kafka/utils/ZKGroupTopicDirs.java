package kafka.utils;

public class ZKGroupTopicDirs extends ZKGroupDirs {
    public String group;
    public String topic;

    public ZKGroupTopicDirs(String group, String topic) {
        super(group);
        this.topic = topic;
    }

    public String consumerOffsetDir() {
        return consumerGroupDir() + "/offsets/" + topic;
    }

    public String consumerOwnerDir() {
        return consumerGroupDir() + "/owners/" + topic;
    }
}