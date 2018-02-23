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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GroupTopicPartition that = (GroupTopicPartition) o;

        if (group != null ? !group.equals(that.group) : that.group != null) return false;
        return topicPartition != null ? topicPartition.equals(that.topicPartition) : that.topicPartition == null;
    }

    @Override
    public int hashCode() {
        int result = group != null ? group.hashCode() : 0;
        result = 31 * result + (topicPartition != null ? topicPartition.hashCode() : 0);
        return result;
    }
}
