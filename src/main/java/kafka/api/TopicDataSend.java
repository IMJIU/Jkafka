package kafka.api;

import kafka.network.MultiSend;
import kafka.network.Send;
import kafka.utils.Utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

import static kafka.api.ApiUtils.*;

/**
 * @author zhoulf
 * @create 2017-10-12 35 11
 **/

public class TopicDataSend extends Send {
    TopicData topicData;

    private Integer size;

    private Integer sent = 0;
    private ByteBuffer buffer ;
    public Send sends;

    @Override
    public boolean complete() {
        return sent >= size;
    }

    public TopicDataSend(TopicData topicData) {
        this.topicData = topicData;
        buffer = ByteBuffer.allocate(topicData.headerSize);
        size = topicData.sizeInBytes();
        writeShortString(buffer, topicData.topic);
        buffer.putInt(topicData.partitionData.size());
        buffer.rewind();
        sends = new MultiSend<PartitionDataSend>(Utils.map(topicData.partitionData, (partition, data) -> new PartitionDataSend(partition, data))) {
            public Integer expectedBytesToWrite() {
                return topicData.sizeInBytes() - topicData.headerSize;
            }
        };
    }


    public Integer writeTo(GatheringByteChannel channel) throws IOException {
        expectIncomplete();
        int written = 0;
        if (buffer.hasRemaining())
            written += channel.write(buffer);
        if (!buffer.hasRemaining() && !sends.complete()) {
            written += sends.writeTo(channel);
        }
        sent += written;
        return written;
    }
}
