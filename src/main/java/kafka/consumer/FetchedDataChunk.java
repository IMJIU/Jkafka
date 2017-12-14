package kafka.consumer;

import kafka.message.ByteBufferMessageSet;

/**
 * @author zhoulf
 * @create 2017-10-30 17:30
 **/
public class FetchedDataChunk {
    public ByteBufferMessageSet messages;
    public PartitionTopicInfo topicInfo;
    public Long fetchOffset;

    public FetchedDataChunk(ByteBufferMessageSet messages, PartitionTopicInfo topicInfo, Long fetchOffset) {
        this.messages = messages;
        this.topicInfo = topicInfo;
        this.fetchOffset = fetchOffset;
    }
}
