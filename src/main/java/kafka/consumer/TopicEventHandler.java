package kafka.consumer;

import java.util.List;


public interface TopicEventHandler<T> {

    void handleTopicEvent(List<T> allTopics);

}
