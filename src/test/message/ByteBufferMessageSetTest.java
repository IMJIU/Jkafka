package message;

import com.google.common.collect.Lists;
import kafka.message.*;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Administrator on 2017/3/26.
 */
public class ByteBufferMessageSetTest extends BaseMessageSetTest {
    public MessageSet createMessageSet(List<Message> messages) {
        return new ByteBufferMessageSet(CompressionCodec.NoCompressionCodec, messages);
    }


    @Test
    public void testValidBytes() {
        {
            ByteBufferMessageSet messages = new ByteBufferMessageSet(CompressionCodec.NoCompressionCodec, new Message("hello".getBytes()), new Message("there".getBytes()));
            ByteBuffer buffer = ByteBuffer.allocate(messages.sizeInBytes() + 2);
            buffer.put(messages.buffer);
            short s = 4;
            buffer.putShort(s);
            ByteBufferMessageSet messagesPlus = new ByteBufferMessageSet(buffer);
            Assert.assertEquals("Adding invalid bytes shouldn't change byte count", messages.validBytes(), messagesPlus.validBytes());
        }

        // test valid bytes on empty ByteBufferMessageSet
        {
            Assert.assertEquals("Valid bytes on an empty ByteBufferMessageSet should return 0", new Integer(0),
                    ((ByteBufferMessageSet) MessageSet.Empty).validBytes());
        }
    }

    @Test
    public void testValidBytesWithCompression() {
        {
            ByteBufferMessageSet messages = new ByteBufferMessageSet(CompressionCodec.GZIPCompressionCodec, new Message("hello".getBytes()), new Message("there".getBytes()));
            ByteBuffer buffer = ByteBuffer.allocate(messages.sizeInBytes() + 2);
            buffer.put(messages.buffer);
            buffer.putShort((short) 4);
            ByteBufferMessageSet messagesPlus = new ByteBufferMessageSet(buffer);
            Assert.assertEquals("Adding invalid bytes shouldn't change byte count", messages.validBytes(), messagesPlus.validBytes());
        }
    }

    @Test
    public void testEquals() {
        ByteBufferMessageSet messages = new ByteBufferMessageSet(CompressionCodec.GZIPCompressionCodec, new Message("hello".getBytes()), new Message("there".getBytes()));
        ByteBufferMessageSet moreMessages = new ByteBufferMessageSet(CompressionCodec.GZIPCompressionCodec, new Message("hello".getBytes()), new Message("there".getBytes()));

        Assert.assertTrue(messages.equals(moreMessages));

        messages = new ByteBufferMessageSet(CompressionCodec.NoCompressionCodec, new Message("hello".getBytes()), new Message("there".getBytes()));
        moreMessages = new ByteBufferMessageSet(CompressionCodec.NoCompressionCodec, new Message("hello".getBytes()), new Message("there".getBytes()));

        Assert.assertTrue(messages.equals(moreMessages));
    }


    @Test
    public void testIterator() {
        Message[] arr = new Message[]{
                new Message("msg1".getBytes()),
                new Message("msg2".getBytes()),
                new Message("msg3".getBytes())
        };
        List<Message> messageList = Arrays.asList(arr);

        // test for uncompressed regular messages
        {
            ByteBufferMessageSet messageSet = new ByteBufferMessageSet(CompressionCodec.NoCompressionCodec, messageList);
            TestUtils.checkEquals(messageList.iterator(), TestUtils.getMessageIterator(messageSet.iterator()));
            //make sure ByteBufferMessageSet is re-iterable.
            TestUtils.checkEquals(messageList.iterator(), TestUtils.getMessageIterator(messageSet.iterator()));

            //make sure shallow iterator is the same as deep iterator
            TestUtils.checkEquals(TestUtils.getMessageIterator(messageSet.shallowIterator()),
                    TestUtils.getMessageIterator(messageSet.iterator()));
        }

        // test for compressed regular messages
        {
            ByteBufferMessageSet messageSet = new ByteBufferMessageSet(CompressionCodec.GZIPCompressionCodec, messageList);
            TestUtils.checkEquals(messageList.iterator(), TestUtils.getMessageIterator(messageSet.iterator()));
            //make sure ByteBufferMessageSet is re-iterable.
            TestUtils.checkEquals(messageList.iterator(), TestUtils.getMessageIterator(messageSet.iterator()));
            verifyShallowIterator(messageSet);
        }

        // test for mixed empty and non-empty messagesets uncompressed
        {
            List<Message> emptyMessageList = null;
            ByteBufferMessageSet emptyMessageSet = new ByteBufferMessageSet(CompressionCodec.NoCompressionCodec, emptyMessageList);
            ByteBufferMessageSet regularMessgeSet = new ByteBufferMessageSet(CompressionCodec.NoCompressionCodec, messageList);
            ByteBuffer buffer = ByteBuffer.allocate(emptyMessageSet.buffer.limit() + regularMessgeSet.buffer.limit());
            buffer.put(emptyMessageSet.buffer);
            buffer.put(regularMessgeSet.buffer);
            buffer.rewind();
            ByteBufferMessageSet mixedMessageSet = new ByteBufferMessageSet(buffer);
            TestUtils.checkEquals(messageList.iterator(), TestUtils.getMessageIterator(mixedMessageSet.iterator()));
            //make sure ByteBufferMessageSet is re-iterable.
            TestUtils.checkEquals(messageList.iterator(), TestUtils.getMessageIterator(mixedMessageSet.iterator()));
            //make sure shallow iterator is the same as deep iterator
            TestUtils.checkEquals(TestUtils.getMessageIterator(mixedMessageSet.shallowIterator()),
                    TestUtils.getMessageIterator(mixedMessageSet.iterator()));
        }

        // test for mixed empty and non-empty messagesets compressed
        {
            List<Message> emptyMessageList = null;
            ByteBufferMessageSet emptyMessageSet = new ByteBufferMessageSet(CompressionCodec.GZIPCompressionCodec, emptyMessageList);
            ByteBufferMessageSet regularMessgeSet = new ByteBufferMessageSet(CompressionCodec.GZIPCompressionCodec, messageList);
            ByteBuffer buffer = ByteBuffer.allocate(emptyMessageSet.buffer.limit() + regularMessgeSet.buffer.limit());
            buffer.put(emptyMessageSet.buffer);
            buffer.put(regularMessgeSet.buffer);
            buffer.rewind();
            ByteBufferMessageSet mixedMessageSet = new ByteBufferMessageSet(buffer);
            TestUtils.checkEquals(messageList.iterator(), TestUtils.getMessageIterator(mixedMessageSet.iterator()));
            //make sure ByteBufferMessageSet is re-iterable.
            TestUtils.checkEquals(messageList.iterator(), TestUtils.getMessageIterator(mixedMessageSet.iterator()));
            verifyShallowIterator(mixedMessageSet);
        }
    }

    @Test
    public void testOffsetAssignment() {
        ByteBufferMessageSet messages = new ByteBufferMessageSet(CompressionCodec.NoCompressionCodec,
                new Message("hello".getBytes()),
                new Message("there".getBytes()),
                new Message("beautiful".getBytes()));
        ByteBufferMessageSet compressedMessages = new ByteBufferMessageSet(CompressionCodec.GZIPCompressionCodec,
                messages.toMessageList());
        // check uncompressed offsets
        checkOffsets(messages, 0L);
        long offset = 1234567;
        checkOffsets(messages.assignOffsets(new AtomicLong(offset), CompressionCodec.NoCompressionCodec), offset);

        // check compressed messages
        checkOffsets(compressedMessages, 0L);
        checkOffsets(compressedMessages.assignOffsets(new AtomicLong(offset), CompressionCodec.GZIPCompressionCodec), offset);
    }

    /* check that offsets are assigned based on byte offset from the given base offset */
    public void checkOffsets(ByteBufferMessageSet messages, Long baseOffset) {
        Long offset = baseOffset;
        for (MessageAndOffset entry : messages) {
            Assert.assertEquals("Unexpected offset in message set iterator", offset, entry.offset);
            offset += 1;
        }
    }

    public void verifyShallowIterator(ByteBufferMessageSet messageSet) {
        //make sure the offsets returned by a shallow iterator is a subset of that of a deep iterator
        Iterator<MessageAndOffset> it1 = messageSet.shallowIterator();
        List<Long> shallowOffsets = Lists.newArrayList();
        while (it1.hasNext()) {
            shallowOffsets.add(it1.next().offset);
        }
        Iterator<MessageAndOffset> it2 = messageSet.iterator();
        List<Long> deepOffsets = Lists.newArrayList();
        while (it2.hasNext()) {
            deepOffsets.add(it2.next().offset);
        }
        Assert.assertTrue(deepOffsets.containsAll(shallowOffsets));
    }
}
