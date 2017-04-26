package kafka.network;

import kafka.annotation.nonthreadsafe;
import kafka.utils.Logging;
import kafka.utils.Utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/**
 * Represents a communication between the client and server
 */
@nonthreadsafe
public class BoundedByteBufferReceive extends Receive {

    public Integer maxSize;

    public BoundedByteBufferReceive(Integer maxSize) {
        this.maxSize = maxSize;
    }

    public BoundedByteBufferReceive() {
        this(Integer.MAX_VALUE);
    }

    private ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
    private ByteBuffer contentBuffer = null;


    public Boolean complete = false;

    @Override
    public String loggerName() {
        return this.getClass().getName();
    }

    /**
     * Get the content buffer for this transmission
     */
    public ByteBuffer buffer() {
        expectComplete();
        return contentBuffer;
    }

    /**
     * Read the bytes in this response from the given channel
     */
    public Integer readFrom(ReadableByteChannel channel) {
        expectIncomplete();
        Integer read = 0;
        try {
            // have we read the request size yet?;
            if (sizeBuffer.remaining() > 0)

                read += Utils.read(channel, sizeBuffer);

            // have we allocated the request buffer yet?;
            if (contentBuffer == null && !sizeBuffer.hasRemaining()) {
                sizeBuffer.rewind();
                Integer size = sizeBuffer.getInt();
                if (size <= 0)
                    throw new InvalidRequestException(String.format("%d is not a valid request size.", size));
                if (size > maxSize)
                    throw new InvalidRequestException(String.format("Request of length %d is not valid, it is larger than the maximum size of %d bytes.", size, maxSize));
                contentBuffer = byteBufferAllocate(size);
            }
            // if we have a buffer read some stuff into it;
            if (contentBuffer != null) {
                read = Utils.read(channel, contentBuffer);
                // did we get everything?;
                if (!contentBuffer.hasRemaining()) {
                    contentBuffer.rewind();
                    complete = true;
                }
            }
        } catch (IOException e) {
            error(e.getMessage(), e);
        }
        return read;
    }

    private ByteBuffer byteBufferAllocate(Integer size) {
        ByteBuffer buffer = null;
        try {
            buffer = ByteBuffer.allocate(size);
        } catch (OutOfMemoryError e) {
            error("OOME with size " + size, e);
            throw e;
        } catch (Exception e) {
            throw e;
        }
        return buffer;
    }

    @Override
    public boolean complete() {
        return complete;
    }
}
