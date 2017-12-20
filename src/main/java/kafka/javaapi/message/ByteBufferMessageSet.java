package kafka.javaapi.message;

import kafka.message.CompressionCodec;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import static kafka.message.CompressionCodec.*;

/**
 * @author zhoulf
 * @create 2017-12-19 13 20
 **/

public class ByteBufferMessageSet extends MessageSet {
    public ByteBuffer buffer;
    private kafka.message.ByteBufferMessageSet underlying = new kafka.message.ByteBufferMessageSet(buffer);

    public ByteBufferMessageSet(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public ByteBufferMessageSet(CompressionCodec compressionCodec, List<Message> messages) {
        // due to SI-4141 which affects Scala 2.8.1, implicits are not visible in constructors and must be used explicitly;
        this(new kafka.message.ByteBufferMessageSet(compressionCodec, new AtomicLong(0), messages).buffer);
    }

    public ByteBufferMessageSet(List<Message> messages) {
        this(CompressionCodec.NoCompressionCodec, messages);
    }

    public Integer validBytes() {
        return underlying.validBytes();
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    @Override
    public Iterator<MessageAndOffset> iterator() {
        return new java.util.Iterator<MessageAndOffset>() {
            Iterator<MessageAndOffset> underlyingIterator = underlying.iterator();

            @Override
            public boolean hasNext() {
                return underlyingIterator.hasNext();
            }

            @Override
            public MessageAndOffset next() {
                return underlyingIterator.next();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("remove API on MessageSet is not supported");
            }

            @Override
            public String toString() {
                return underlying.toString();
            }
        };
    }

    public Integer sizeInBytes() {
        return underlying.sizeInBytes();
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof ByteBufferMessageSet) {
            return buffer.equals(((ByteBufferMessageSet) other).buffer);
        }
        return false;
    }


    @Override
    public int hashCode() {
        return buffer.hashCode();
    }
}
