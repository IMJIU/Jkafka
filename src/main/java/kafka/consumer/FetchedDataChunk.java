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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FetchedDataChunk that = (FetchedDataChunk) o;

        if (messages != null ? !messages.equals(that.messages) : that.messages != null) return false;
        if (topicInfo != null ? !topicInfo.equals(that.topicInfo) : that.topicInfo != null) return false;
        return fetchOffset != null ? fetchOffset.equals(that.fetchOffset) : that.fetchOffset == null;
    }

    @Override
    public int hashCode() {
        int result = messages != null ? messages.hashCode() : 0;
        result = 31 * result + (topicInfo != null ? topicInfo.hashCode() : 0);
        result = 31 * result + (fetchOffset != null ? fetchOffset.hashCode() : 0);
        return result;
    }
}
