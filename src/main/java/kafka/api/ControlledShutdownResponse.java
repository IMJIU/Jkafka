package kafka.api;

import com.google.common.collect.Sets;
import kafka.common.ErrorMapping;
import kafka.log.TopicAndPartition;

import java.nio.ByteBuffer;
import java.util.Set;

/**
 * @author zhoulf
 * @create 2017-11-28 09 14
 **/
import static kafka.api.ApiUtils.*;

public class ControlledShutdownResponse extends RequestOrResponse {
    public int correlationId;
    public short errorCode = ErrorMapping.NoError;
    public Set<TopicAndPartition> partitionsRemaining;

    public ControlledShutdownResponse(int correlationId, short errorCode, Set<TopicAndPartition> partitionsRemaining) {
        this.correlationId = correlationId;
        this.errorCode = errorCode;
        this.partitionsRemaining = partitionsRemaining;
    }

    public static ControlledShutdownResponse readFrom(ByteBuffer buffer) {
        int correlationId = buffer.getInt();
        short errorCode = buffer.getShort();
        int numEntries = buffer.getInt();

        Set<TopicAndPartition> partitionsRemaining = Sets.newHashSet();
        for (int i = 0; i < numEntries; i++) {
            String topic = readShortString(buffer);
            int partition = buffer.getInt();
            partitionsRemaining.add(new TopicAndPartition(topic, partition));
        }
        return new ControlledShutdownResponse(correlationId, errorCode, partitionsRemaining);
    }


    public Integer sizeInBytes() {
        int size =
                4 /* correlation id */ +
                        2 /* error code */ +
                        4 /* number of responses */;
        for (TopicAndPartition topicAndPartition : partitionsRemaining) {
            size +=
                    2 + topicAndPartition.topic.length() /* topic */ +
                            4 /* partition */;
        }
        return size;
    }

    public void writeTo(ByteBuffer buffer) {
        buffer.putInt(correlationId);
        buffer.putShort(errorCode);
        buffer.putInt(partitionsRemaining.size());
        for (TopicAndPartition topicAndPartition : partitionsRemaining) {
            writeShortString(buffer, topicAndPartition.topic);
            buffer.putInt(topicAndPartition.partition);
        }
    }

    @Override
    public String describe(Boolean details) {
        return this.toString();
    }

}
