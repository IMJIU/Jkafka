package kafka.message;/**
 * Created by zhoulf on 2017/3/23.
 */

import com.google.common.collect.Lists;
import kafka.utils.IteratorTemplate;
import org.apache.commons.collections.CollectionUtils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author
 * @create 2017-03-23 12:57
 **/
public class ByteBufferMessageSet extends MessageSet {
    public ByteBuffer buffer;
    private int shallowValidByteCount = -1;

    public ByteBufferMessageSet(ByteBuffer buffer) {
        this.buffer = buffer;
    }
    private static ByteBuffer create(AtomicLong offsetCounter, CompressionCodec compressionCodec, List<Message> messages) {
        return create0(offsetCounter, compressionCodec, messages);
    }

    private static ByteBuffer create0(AtomicLong offsetCounter, CompressionCodec compressionCodec, List<Message> messages) {
        if (CollectionUtils.isEmpty(messages)) {
            return MessageSet.Empty.buffer;
        } else if (compressionCodec == CompressionCodec.NoCompressionCodec) {
            ByteBuffer buffer = ByteBuffer.allocate(MessageSet.messageSetSize(messages));
            for (Message message : messages) {
                writeMessage(buffer, message, offsetCounter.getAndIncrement());
            }
            buffer.rewind();
            return buffer;
        } else {
            ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream(MessageSet.messageSetSize(messages));
            DataOutputStream output = new DataOutputStream(CompressionFactory.apply(compressionCodec, byteArrayStream));
            long offset = -1L;
            try {
                try {
                    for (Message message : messages) {
                        offset = offsetCounter.getAndIncrement();
                        output.writeLong(offset);
                        output.writeInt(message.size());
                        output.write(message.buffer.array(), message.buffer.arrayOffset(), message.buffer.limit());
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } finally {
                try {
                    output.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            byte[] bytes = byteArrayStream.toByteArray();
            Message message = new Message(bytes, compressionCodec);
            ByteBuffer buffer = ByteBuffer.allocate(message.size() + MessageSet.LogOverhead);
            writeMessage(buffer, message, offset);
            buffer.rewind();
            return buffer;
        }
    }

    private static ByteBuffer create(AtomicLong offsetCounter, CompressionCodec compressionCodec, Message... messages) {
        return create0(offsetCounter, compressionCodec, Lists.newArrayList(messages));
    }

    public static ByteBufferMessageSet decompress(Message message) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        InputStream inputStream = new ByteBufferBackedInputStream(message.payload());
        byte[] intermediateBuffer = new byte[1024];
        InputStream compressed = CompressionFactory.apply(message.compressionCodec(), inputStream);
        try {
            try {
                int len;
                while ((len = compressed.read(intermediateBuffer)) > 0) {
                    outputStream.write(intermediateBuffer, 0, len);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        } finally {
            try {
                compressed.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        ByteBuffer outputBuffer = ByteBuffer.allocate(outputStream.size());
        outputBuffer.put(outputStream.toByteArray());
        outputBuffer.rewind();
        return new ByteBufferMessageSet(outputBuffer);
    }

    private static void writeMessage(ByteBuffer buffer, Message message, Long offset) {
        buffer.putLong(offset);
        buffer.putInt(message.size());
        buffer.put(message.buffer);
        message.buffer.rewind();
    }

    /**
     * A sequence of messages stored in a byte buffer
     * <p>
     * There are two ways to create a ByteBufferMessageSet
     * <p>
     * Option 1: From a ByteBuffer which already contains the serialized message set. Consumers will use this method.
     * <p>
     * Option 2: Give it a list of messages along with instructions relating to serialization format. Producers will use this method.
     */
    public ByteBufferMessageSet(CompressionCodec compressionCodec, Message... messages) {
        this(ByteBufferMessageSet.create(new AtomicLong(0), compressionCodec, messages));
    }
    public ByteBufferMessageSet(CompressionCodec compressionCodec, List<Message> messages) {
        this(ByteBufferMessageSet.create(new AtomicLong(0), compressionCodec, messages));
    }
    public ByteBufferMessageSet(CompressionCodec compressionCodec, AtomicLong offsetCounter, Message... messages) {
        this(ByteBufferMessageSet.create(offsetCounter, compressionCodec, messages));
    }


    public ByteBufferMessageSet(Message... messages) {
        this(CompressionCodec.NoCompressionCodec, new AtomicLong(0), messages);
    }
    public ByteBufferMessageSet(List<Message>messages) {
        this(CompressionCodec.NoCompressionCodec, messages);
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    private Integer shallowValidBytes() {
        if (shallowValidByteCount < 0) {
            int bytes = 0;
            Iterator<MessageAndOffset> it = this.internalIterator(true);
            while (it.hasNext()) {
                MessageAndOffset messageAndOffset = it.next();
                bytes += MessageSet.entrySize(messageAndOffset.message);
            }
            this.shallowValidByteCount = bytes;
        }
        return shallowValidByteCount;
    }



    /**
     * Write the messages in this set to the given channel
     */
    @Override
    public Integer writeTo(GatheringByteChannel channel, Long offset, Integer maxSize) throws IOException {
        // Ignore offset and size from input. We just want to write the whole buffer to the channel.
        buffer.mark();
        int written = 0;
        try {
            while (written < sizeInBytes()) {
                written += channel.write(buffer);
            }
            buffer.reset();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return written;
    }

    /**
     * default iterator that iterates over decompressed messages
     */

    public Iterator<MessageAndOffset> iterator() {
        return internalIterator(false);
    }


    /**
     * iterator over compressed messages without decompressing
     */
    public Iterator<MessageAndOffset> shallowIterator() {
        return internalIterator(true);
    }

    /**
     * When flag isShallow is set to be true, we do a shallow iteration: just traverse the first level of messages.
     **/
    @SuppressWarnings("unchecked")
    private Iterator<MessageAndOffset> internalIterator(final boolean isShallow) {
        return new IteratorTemplate<MessageAndOffset>() {
            ByteBuffer topItBuffer = buffer.slice();
            Iterator<MessageAndOffset> innerIterator = null;

            @Override
            protected MessageAndOffset makeNext() {
                if (isShallow) {
                    return makeNextOuter();
                } else {
                    if (innerDone())
                        return makeNextOuter();
                    else
                        return innerIterator.next();
                }
            }

            public boolean innerDone() {
                return (innerIterator == null || !innerIterator.hasNext());
            }

            public MessageAndOffset makeNextOuter() {
                // if there isn't at least an offset and size, we are done
                if (topItBuffer.remaining() < 12)
                    return allDone();
                long offset = topItBuffer.getLong();
                int size = topItBuffer.getInt();
                if (size < Message.MinHeaderSize)
                    throw new InvalidMessageException("Message found with corrupt size (" + size + ")");

                // we have an incomplete message
                if (topItBuffer.remaining() < size)
                    return allDone();

                // read the current message and check correctness
                ByteBuffer message = topItBuffer.slice();
                message.limit(size);
                topItBuffer.position(topItBuffer.position() + size);
                Message newMessage = new Message(message);

                if (isShallow) {
                    return new MessageAndOffset(newMessage, offset);
                } else {
                    switch (newMessage.compressionCodec()) {
                        case NoCompressionCodec:
                            innerIterator = null;
                            return new MessageAndOffset(newMessage, offset);
                        default:
                            innerIterator = ByteBufferMessageSet.decompress(newMessage).internalIterator(false);
                            if (!innerIterator.hasNext())
                                innerIterator = null;
                            return makeNext();
                    }
                }
            }
        };
    }

    /**
     * Update the offsets for this message set. This method attempts to do an in-place conversion
     * if there is no compression, but otherwise recopies the messages
     */
    public ByteBufferMessageSet assignOffsets(AtomicLong offsetCounter, CompressionCodec codec) {
        if (codec == CompressionCodec.NoCompressionCodec) {
            // do an in-place conversion
            int position = 0;
            buffer.mark();
            while (position < sizeInBytes() - MessageSet.LogOverhead) {
                buffer.position(position);
                buffer.putLong(offsetCounter.getAndIncrement());
                position += MessageSet.LogOverhead + buffer.getInt();
            }
            buffer.reset();
            return this;
        } else {
            // messages are compressed, crack open the messageset and recompress with correct offset
            Iterator<MessageAndOffset> it = this.internalIterator(false);
            List<Message> list = new ArrayList();
            while (it.hasNext()) {
                list.add(it.next().message);
            }
            return new ByteBufferMessageSet(codec, offsetCounter, list.toArray(new Message[list.size()]));
        }
    }


    /**
     * The total number of bytes in this message set, including any partial trailing messages
     */
    @Override
    public Integer sizeInBytes() {
        return buffer.limit();
    }

    /**
     * The total number of bytes in this message set not including any partial, trailing messages
     */
    public Integer validBytes() {
        return shallowValidBytes();
    }

    /**
     * Two message sets are equal if their respective byte buffers are equal
     */
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
