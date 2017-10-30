package kafka.consumer;

import kafka.message.ByteBufferMessageSet;

/**
 * @author zhoulf
 * @create 2017-10-30 17:30
 **/
public class FetchedDataChunk {
    public ByteBufferMessageSet cmessages;
    public PartitionTopicInfo topicInfo;
    public Long fetchOffset;

    public FetchedDataChunk(ByteBufferMessageSet cmessages, PartitionTopicInfo topicInfo, Long fetchOffset) {
        this.cmessages = cmessages;
        this.topicInfo = topicInfo;
        this.fetchOffset = fetchOffset;
    }
}
