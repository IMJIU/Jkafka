package kafka.network;

import kafka.annotation.nonthreadsafe;
import kafka.api.RequestOrResponse;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.Arrays;
import java.util.Optional;

/**
 * Created by zhoulf on 2017/4/26.
 */

@nonthreadsafe
public class BoundedByteBufferSend extends Send {
    public ByteBuffer buffer;


    private ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
    public Boolean complete = false;

    public BoundedByteBufferSend(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public void init() {
        // Avoid possibility of overflow for 2GB-4 byte buffer;
        if (buffer.remaining() > Integer.MAX_VALUE - sizeBuffer.limit())
            throw new IllegalStateException("Attempt to create a bounded buffer of " + buffer.remaining() + " bytes, " +
                    "but the maximum " +
                    "allowable size for a bounded buffer is " + (Integer.MAX_VALUE - sizeBuffer.limit()) + ".");
        sizeBuffer.putInt(buffer.limit());
        sizeBuffer.rewind();
    }


    @Override
    public boolean complete() {
        return complete;
    }

    public BoundedByteBufferSend(Integer size) {
        this(ByteBuffer.allocate(size));
    }

    public BoundedByteBufferSend(RequestOrResponse request) {
        this(request.sizeInBytes() + (request.requestId.equals(Optional.empty()) ? 2 : 0));
        if (request.requestId.isPresent()) {
            buffer.putShort(request.requestId.get());
        }
        request.writeTo(buffer);
        buffer.rewind();
    }


    public Integer writeTo(GatheringByteChannel channel) {
        expectIncomplete();
        ByteBuffer[] bs = new ByteBuffer[]{sizeBuffer, buffer};
        long written = 0;
        try {
            written = channel.write(bs);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // if we are done, mark it off;
        if (!buffer.hasRemaining())
            complete = true;
        return (int)written;
    }

}
