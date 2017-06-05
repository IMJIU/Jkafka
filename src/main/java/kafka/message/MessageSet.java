package kafka.message;/**
 * Created by zhoulf on 2017/3/22.
 */

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import kafka.utils.Itor;
import kafka.utils.Logging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * Message set helper functions
 */
public abstract class MessageSet extends Logging implements Iterable<MessageAndOffset> {
    public static final int MessageSizeLength = 4;
    public static final int OffsetLength = 8;
    public static final int LogOverhead = MessageSizeLength + OffsetLength;
    public static final ByteBufferMessageSet Empty = new ByteBufferMessageSet(ByteBuffer.allocate(0));

    /**
     * The size of a message set containing the given messages
     */
    public static Integer messageSetSize(Message... messages) {
        int total = 0;
        for (Message m : messages) {
            total += entrySize(m);
        }
        return total;
    }

    public static Integer messageSetSize(List<Message> messages) {
        int total = 0;
        for (Message m : messages) {
            total += entrySize(m);
        }
        return total;
    }

    /**
     * The size of a list of messages
     */
    public static Integer messageSetSize(Iterator<Message> it) {
        int size = 0;
        while (it.hasNext()) {
            size += entrySize(it.next());
        }
        return size;
    }

    /**
     * The size of a size-delimited entry in a message set
     */
    public static Integer entrySize(Message message) {
        return LogOverhead + message.size();
    }


    /**
     * A set of messages with offsets. A message set has a fixed serialized form, though the container
     * for the bytes could be either in-memory or on disk. The format of each message is
     * as follows:
     * 8 byte message offset number
     * 4 byte size containing an integer N
     * N message bytes as described in the Message class
     * <p>
     * /** Write the messages in this set to the given channel starting at the given offset byte.
     * Less than the complete amount may be written, but no more than maxSize can be. The number
     * of bytes written is returned
     */
    public abstract Integer writeTo(GatheringByteChannel channel, Long offset, Integer maxSize) throws IOException;

    /**
     * Provides an iterator over the message/offset pairs in this set
     */
    public abstract Iterator<MessageAndOffset> iterator();

    /**
     * Gives the total size of this message set in bytes
     */
    public abstract Integer sizeInBytes();

    /**
     * Print this message set's contents. If the message set has more than 100 messages, just
     * print the first 100.
     */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(getClass().getSimpleName() + "(");
        Iterator<MessageAndOffset> iter = this.iterator();
        int i = 0;
        while (iter.hasNext() && i < 100) {
            MessageAndOffset message = iter.next();
            builder.append(message);
            if (iter.hasNext())
                builder.append(", ");
            i += 1;
        }
        if (iter.hasNext())
            builder.append("...");
        builder.append(")");
        return builder.toString();
    }


    public List<MessageAndOffset> toMessageAndOffsetList() {
        List<MessageAndOffset> result = Lists.newArrayList();
        Itor.loop(iterator(), m -> result.add(m));
        return result;
    }

    public List<Message> toMessageList() {
        List<Message> result = Lists.newLinkedList();
        Itor.loop(iterator(), m -> result.add(m.message));
        return result;
    }

    public MessageAndOffset head() {
        Iterator<MessageAndOffset> it = iterator();
        if (it.hasNext()) {
            return it.next();
        }
        return null;
    }


    public Iterator<MessageAndOffset> tail() {
        Iterator<MessageAndOffset> it = iterator();
        if (it.hasNext()) {
            it.next();
            return it;
        }
        return null;
    }

    public Optional<MessageAndOffset> last() {
        Iterator<MessageAndOffset> it = iterator();
        MessageAndOffset last = null;
        while (it.hasNext()) {
            last = it.next();
        }
        return Optional.of(last);
    }

    public void printAll(){
        Itor.loop(iterator(),m-> System.out.println(m));
        System.out.println("=====================================");
    }
}
