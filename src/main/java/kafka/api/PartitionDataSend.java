package kafka.api;

import kafka.network.Send;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

/**
 * @author zhoulf
 * @create 2017-10-12 12 14
 **/

public class PartitionDataSend extends Send {
    public Integer partitionId;
    public FetchResponsePartitionData partitionData;
    private ByteBuffer buffer = ByteBuffer.allocate(4 /** partitionId **/ + FetchResponsePartitionData.headerSize);
    private Integer messageSize ;
    private Long messagesSentSize = 0L;

    public PartitionDataSend(java.lang.Integer partitionId, FetchResponsePartitionData partitionData) {
        this.partitionId = partitionId;
        this.partitionData = partitionData;
        messageSize = partitionData.messages.sizeInBytes();
        buffer.putInt(partitionId);
        buffer.putShort(partitionData.error);
        buffer.putLong(partitionData.hw);
        buffer.putInt(partitionData.messages.sizeInBytes());
        buffer.rewind();
    }


    @Override
    public boolean complete() {
        return !buffer.hasRemaining() && messagesSentSize >= messageSize;
    }

    @Override
    public Integer writeTo(GatheringByteChannel channel) {
        int written = 0;
        try {
            if (buffer.hasRemaining())
                written += channel.write(buffer);
            if (!buffer.hasRemaining() && messagesSentSize < messageSize) {
                int bytesSent ;
                bytesSent = partitionData.messages.writeTo(channel, messagesSentSize, new Long(messageSize - messagesSentSize).intValue());
                messagesSentSize += bytesSent;
                written += bytesSent;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return written;
    }
}
