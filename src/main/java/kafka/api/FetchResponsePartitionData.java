package kafka.api;

import kafka.common.ErrorMapping;
import kafka.message.ByteBufferMessageSet;
import kafka.message.MessageSet;

import java.nio.ByteBuffer;

/**
 * @author zhoulf
 * @create 2017-10-11 17 14
 **/

public class FetchResponsePartitionData {
    public Short error = ErrorMapping.NoError;
    public Long hw = -1L;
    public MessageSet messages;
    public static final int headerSize =
            2 + /* error code */
                    8 + /* high watermark */
                    4; /* messageSetSize */
    public Integer sizeInBytes;

    public FetchResponsePartitionData(Short error, Long hw, MessageSet messages) {
        if (error == null) {
            error = ErrorMapping.NoError;
        }
        if (hw == null) {
            hw = -1L;
        }
        this.error = error;
        this.hw = hw;
        sizeInBytes = FetchResponsePartitionData.headerSize + messages.sizeInBytes();
    }

    public static FetchResponsePartitionData readFrom(ByteBuffer buffer) {
        short error = buffer.getShort();
        long hw = buffer.getLong();
        int messageSetSize = buffer.getInt();
        ByteBuffer messageSetBuffer = buffer.slice();
        messageSetBuffer.limit(messageSetSize);
        buffer.position(buffer.position() + messageSetSize);
        return new FetchResponsePartitionData(error, hw, new ByteBufferMessageSet(messageSetBuffer));
    }
}




