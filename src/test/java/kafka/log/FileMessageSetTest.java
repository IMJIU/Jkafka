package kafka.log;

import com.google.common.collect.Lists;
import kafka.message.*;
import kafka.message.BaseMessageSetTest;
import kafka.utils.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Administrator on 2017/3/27.
 */
public class FileMessageSetTest extends BaseMessageSetTest {

    FileMessageSet messageSet = createMessageSet(messages);

    //"abcd" "efgh" "ijkl"
    public FileMessageSet createMessageSet(List<Message> messages) {
        try {
            FileMessageSet set = new FileMessageSet(TestUtils.tempFile());
            set.append(new ByteBufferMessageSet(CompressionCodec.NoCompressionCodec, messages));
            set.flush();
            return set;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;

    }

    /**
     * Test that the cached size variable matches the actual file size as we append messages
     * 增加一条记录，channel实际长度与size是否相等
     */
    @Test
    public void testFileSize() throws IOException {
        Assert.assertEquals(messageSet.channel.size(), messageSet.sizeInBytes().longValue());
        for (int i = 0; i < 20; i++) {
            messageSet.append(TestUtils.singleMessageSet("abcd".getBytes()));
            Assert.assertEquals(messageSet.channel.size(), messageSet.sizeInBytes().longValue());
        }
    }

    /**
     * Test that adding invalid bytes to the end of the log doesn't break iteration
     * 增加无效字符  是否影响迭代器
     */
    @Test
    public void testIterationOverPartialAndTruncation() throws IOException {
        testPartialWrite(0, messageSet);
        testPartialWrite(2, messageSet);
        testPartialWrite(4, messageSet);
        testPartialWrite(5, messageSet);
        testPartialWrite(6, messageSet);
    }

    private void testPartialWrite(Integer size, FileMessageSet messageSet) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(size);
        for (int i = 0; i < size; i++)
            buffer.put((byte) 0);
        buffer.rewind();
        messageSet.channel.write(buffer);
        // appending those bytes should not change the contents
        TestUtils.checkEquals(messages.iterator(), messageSet.toMessageList().iterator());
    }

    /**
     * Iterating over the file does file reads but shouldn't change the position of the underlying FileChannel.
     * iterator遍历后，position是否有变化。（不变）
     */
    @Test
    public void testIterationDoesntChangePosition() throws IOException {
        long position = messageSet.channel.position();//90
        TestUtils.checkEquals(messages.iterator(), messageSet.toMessageList().iterator());
        Assert.assertEquals(position, messageSet.channel.position());
    }

    /**
     * Test a simple append and read.
     * fileMessage 读取测试
     */
    @Test
    public void testRead() {
        FileMessageSet read = messageSet.read(0, messageSet.sizeInBytes());
        TestUtils.checkEquals(messageSet.iterator(), read.iterator());
        List<MessageAndOffset> items = read.toMessageAndOffsetList();
        MessageAndOffset sec = read.tail().next();

        //读取第二个和之后所有元素
        read = messageSet.read(MessageSet.entrySize(sec.message), messageSet.sizeInBytes());
        TestUtils.assertEquals("Try a read starting from the second message", items.subList(1, items.size()).iterator(), read.iterator());

        //读取第二个
        read = messageSet.read(MessageSet.entrySize(sec.message), MessageSet.entrySize(sec.message));
        TestUtils.assertEquals("Try a read of a single message starting from the second message", items.subList(1, 2).iterator(), read.iterator());
    }

    /**
     * Test the MessageSet.searchFor API.
     * 搜索
     */
    @Test
    public void testSearch() {
        // append a new message with a high offset
        Message lastMessage = new Message("test".getBytes());
        messageSet.append(new ByteBufferMessageSet(CompressionCodec.NoCompressionCodec, new AtomicLong(50), lastMessage));
        int position = 0;
        //offset >= targetOffset 直接返回offset
        Assert.assertEquals("Should be able to find the first message by its offset",
                new OffsetPosition(0L, position), messageSet.searchFor(0L, 0));
        position += MessageSet.entrySize(messageSet.head().message);

        Assert.assertEquals("Should be able to find second message when starting from 0",
                new OffsetPosition(1L, position), messageSet.searchFor(1L, 0));

        Assert.assertEquals("Should be able to find second message starting from its offset",
                new OffsetPosition(1L, position), messageSet.searchFor(1L, position));

        Iterator<MessageAndOffset> tailIt = messageSet.tail();
        tailIt.next();
        position += MessageSet.entrySize(messageSet.tail().next().message) + MessageSet.entrySize(tailIt.next().message);

        Assert.assertEquals("Should be able to find fourth message from a non-existant offset",
                new OffsetPosition(50L, position), messageSet.searchFor(3L, position));

        Assert.assertEquals("Should be able to find fourth message by correct offset",
                new OffsetPosition(50L, position), messageSet.searchFor(50L, position));
    }

    /**
     * Test that the message set iterator obeys start and end slicing
     * 截取 与 遍历 是否一致
     */
    @Test
    public void testIteratorWithLimits() {
        MessageAndOffset second = messageSet.tail().next();
        Integer start = messageSet.searchFor(1L, 0).position;
        Integer size = second.message.size();
        FileMessageSet slice = messageSet.read(start, size);
        Assert.assertEquals(Lists.newArrayList(second.message), slice.toMessageList());
    }

    /**
     * Test the truncateTo method lops off messages and appropriately updates the size
     * 测试截断（保留第一个）
     */
    @Test
    public void testTruncate() {
        MessageAndOffset message = messageSet.head();
        Integer end = messageSet.searchFor(1L, 0).position;
        messageSet.truncateTo(end);
        Assert.assertEquals(Lists.newArrayList(message.message), messageSet.toMessageList());
        Assert.assertEquals(MessageSet.entrySize(message.message), messageSet.sizeInBytes());
    }
}
