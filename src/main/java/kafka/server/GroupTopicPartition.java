package kafka.server;

import kafka.log.TopicAndPartition;

/**
 * Created by Administrator on 2017/6/1.
 */
public class GroupTopicPartition {
    public String group;
    public TopicAndPartition topicPartition;

    public GroupTopicPartition(String group, TopicAndPartition topicPartition) {
        this.group = group;
        this.topicPartition = topicPartition;
    }

    public GroupTopicPartition(String group, String topic, Integer partition) {
        this(group, new TopicAndPartition(topic, partition));
    }

    public String toString() {
        return String.format("[%s,%s,%d]", group, topicPartition.topic, topicPartition.partition);

    }
}
