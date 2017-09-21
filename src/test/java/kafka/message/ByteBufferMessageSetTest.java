package kafka.message;

import com.google.common.collect.Lists;
import kafka.utils.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
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

    /**
     * 测试有效字节数 和 空时字节数是否为0  validBytes
     * 增加几个无效字节测试有效字节数
     */
    @Test
    public void testValidBytes() {
        {
            ByteBufferMessageSet messages = new ByteBufferMessageSet(CompressionCodec.NoCompressionCodec, new Message("hello1111111".getBytes()), new Message("there1111111".getBytes()));
            ByteBuffer buffer = ByteBuffer.allocate(messages.sizeInBytes() + 2);
            buffer.put(messages.buffer);
            short s = 4;
            buffer.putShort(s);
            messages.buffer.rewind();
            buffer.rewind();
            ByteBufferMessageSet messagesPlus = new ByteBufferMessageSet(buffer);
            Assert.assertEquals("Adding invalid bytes shouldn't change byte count", messages.validBytes(), messagesPlus.validBytes());
            System.out.println(messages.validBytes());
        }

        // test valid bytes on empty ByteBufferMessageSet
        {
            Assert.assertEquals("Valid bytes on an empty ByteBufferMessageSet should return 0",
                    new Integer(0), MessageSet.Empty.validBytes());
        }
    }

    /**
     * 测试压缩状态下的有效字节数
     */
    @Test
    public void testValidBytesWithCompression() {
        ByteBufferMessageSet messages = new ByteBufferMessageSet(CompressionCodec.GZIPCompressionCodec, new Message("hello1111111".getBytes()), new Message("there1111111".getBytes()));
        ByteBuffer buffer = ByteBuffer.allocate(messages.sizeInBytes() + 2);
        buffer.put(messages.buffer);
        buffer.putShort((short) 4);
        messages.buffer.rewind();
        buffer.rewind();
        ByteBufferMessageSet messagesPlus = new ByteBufferMessageSet(buffer);
        Assert.assertEquals("Adding invalid bytes shouldn't change byte count", messages.validBytes(), messagesPlus.validBytes());
        System.out.println(messages.validBytes());
    }

    /**
     * 测试message 是否相等 equals
     */
    @Test
    public void testEquals() {
        ByteBufferMessageSet messages = new ByteBufferMessageSet(CompressionCodec.GZIPCompressionCodec, new Message("hello".getBytes()), new Message("there".getBytes()));
        ByteBufferMessageSet moreMessages = new ByteBufferMessageSet(CompressionCodec.GZIPCompressionCodec, new Message("hello".getBytes()), new Message("there".getBytes()));

        Assert.assertTrue(messages.equals(moreMessages));

        messages = new ByteBufferMessageSet(CompressionCodec.NoCompressionCodec, new Message("hello".getBytes()), new Message("there".getBytes()));
        moreMessages = new ByteBufferMessageSet(CompressionCodec.NoCompressionCodec, new Message("hello".getBytes()), new Message("there".getBytes()));

        Assert.assertTrue(messages.equals(moreMessages));
    }

    /**
     * 测试迭代器
     */
    @Test
    public void testIterator() {
        List<Message> messageList = Lists.newArrayList(
                new Message("msg1".getBytes()),
                new Message("msg2".getBytes()),
                new Message("msg3".getBytes()));

        // test for uncompressed regular messages 测试无压缩
        {
            ByteBufferMessageSet messageSet = new ByteBufferMessageSet(CompressionCodec.NoCompressionCodec, messageList);
            TestUtils.checkEquals(messageList.iterator(), messageSet.toMessageList().iterator());
            //make sure ByteBufferMessageSet is re-iterable. 重复校验一次
            TestUtils.checkEquals(messageList.iterator(), messageSet.toMessageList().iterator());

            //make sure shallow iterator is the same as deep iterator 确保浅迭代器也是一致
            TestUtils.checkEquals(TestUtils.getMessageIterator(messageSet.shallowIterator()),
                    messageSet.toMessageList().iterator());
        }

        // test for compressed regular messages 测试有压缩
        {
            ByteBufferMessageSet messageSet = new ByteBufferMessageSet(CompressionCodec.GZIPCompressionCodec, messageList);
            TestUtils.checkEquals(messageList.iterator(), messageSet.toMessageList().iterator());
            //make sure ByteBufferMessageSet is re-iterable. 重复校验一次
            TestUtils.checkEquals(messageList.iterator(), messageSet.toMessageList().iterator());
            //校验浅迭代器和普通迭代器（默认为深度迭代（解压缩））
            verifyShallowIterator(messageSet);
        }

        // test for mixed empty and non-empty messagesets uncompressed 普通msg加入空msg测试
        {
            List<Message> emptyMessageList = null;
            ByteBufferMessageSet emptyMessageSet = new ByteBufferMessageSet(CompressionCodec.NoCompressionCodec, emptyMessageList);
            ByteBufferMessageSet regularMessgeSet = new ByteBufferMessageSet(CompressionCodec.NoCompressionCodec, messageList);
            ByteBuffer buffer = ByteBuffer.allocate(emptyMessageSet.buffer.limit() + regularMessgeSet.buffer.limit());
            buffer.put(emptyMessageSet.buffer);
            buffer.put(regularMessgeSet.buffer);
            buffer.rewind();
            ByteBufferMessageSet mixedMessageSet = new ByteBufferMessageSet(buffer);
            //迭代器是否一致
            TestUtils.checkEquals(messageList.iterator(), mixedMessageSet.toMessageList().iterator());
            //make sure ByteBufferMessageSet is re-iterable.重复校验
            TestUtils.checkEquals(messageList.iterator(), mixedMessageSet.toMessageList().iterator());
            //make sure shallow iterator is the same as deep iterator 校验深迭代和浅迭代
            TestUtils.checkEquals(TestUtils.getMessageIterator(mixedMessageSet.shallowIterator()),
                    mixedMessageSet.toMessageList().iterator());
        }

        // test for mixed empty and non-empty messagesets compressed 普通msg加入空msg压缩测试
        {
            List<Message> emptyMessageList = null;
            ByteBufferMessageSet emptyMessageSet = new ByteBufferMessageSet(CompressionCodec.GZIPCompressionCodec, emptyMessageList);
            ByteBufferMessageSet regularMessgeSet = new ByteBufferMessageSet(CompressionCodec.GZIPCompressionCodec, messageList);
            ByteBuffer buffer = ByteBuffer.allocate(emptyMessageSet.buffer.limit() + regularMessgeSet.buffer.limit());
            buffer.put(emptyMessageSet.buffer);
            buffer.put(regularMessgeSet.buffer);
            buffer.rewind();
            ByteBufferMessageSet mixedMessageSet = new ByteBufferMessageSet(buffer);
            //迭代器是否一致
            TestUtils.checkEquals(messageList.iterator(), mixedMessageSet.toMessageList().iterator());
            //make sure ByteBufferMessageSet is re-iterable.重复校验
            TestUtils.checkEquals(messageList.iterator(), mixedMessageSet.toMessageList().iterator());
            //校验深迭代和浅迭代
            verifyShallowIterator(mixedMessageSet);
        }
    }

    /**
     * 测试注定偏移ID
     */
    @Test
    public void testOffsetAssignment() {
        ByteBufferMessageSet messages = new ByteBufferMessageSet(CompressionCodec.NoCompressionCodec,
                new Message("hello".getBytes()),
                new Message("there".getBytes()),
                new Message("beautiful".getBytes()));
        ByteBufferMessageSet compressedMessages = new ByteBufferMessageSet(CompressionCodec.GZIPCompressionCodec,
                messages.toMessageList());
        // check uncompressed offsets 检查偏移量 从0开始递增
        checkOffsets(messages, 0L);
        long offset = 1234567;
        //检查偏移量 从1234567开始递增
        checkOffsets(messages.assignOffsets(new AtomicLong(offset), CompressionCodec.NoCompressionCodec), offset);

        // check compressed messages  压缩 检查偏移量 从0开始递增
        checkOffsets(compressedMessages, 0L);
        //压缩 检查偏移量 从1234567开始递增
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
