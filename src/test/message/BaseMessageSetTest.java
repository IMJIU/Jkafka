package message;

import com.google.common.collect.Lists;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import kafka.message.MessageSet;
import org.junit.Assert;
import org.junit.Test;

import static message.TestUtils.checkEquals;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Administrator on 2017/3/26.
 */
public abstract class BaseMessageSetTest {
    private Message[] arr = new Message[]{new Message("abcd".getBytes()), new Message("efgh".getBytes()), new Message("ijkl".getBytes())};
    public List<Message> messages = new ArrayList<Message>(Arrays.asList(arr));

    public abstract MessageSet createMessageSet(List<Message> messages);

    @Test
    public void testWrittenEqualsRead() {
        MessageSet messageSet = createMessageSet(messages);
        Iterator<MessageAndOffset> it = messageSet.iterator();
        List<Message> list = Lists.newArrayList();
        while (it.hasNext()) {
            list.add(it.next().message);
        }
        checkEquals(messages.iterator(), list.iterator());
    }

    @Test
    public void testIteratorIsConsistent() {
        MessageSet m = createMessageSet(messages);
        // two iterators over the same set should give the same results
        checkEquals(m.iterator(), m.iterator());
    }

    @Test
    public void testSizeInBytes() {
        Assert.assertEquals("Empty message set should have 0 bytes.",
                new Integer(0),
                createMessageSet(new ArrayList<Message>()).sizeInBytes());
        Assert.assertEquals("Predicted size should equal actual size.",
                MessageSet.messageSetSize(messages),
                createMessageSet(messages).sizeInBytes());
    }

    @Test
    public void testWriteTo() {
        // test empty message set
        testWriteToWithMessageSet(createMessageSet(new ArrayList<Message>()));
        testWriteToWithMessageSet(createMessageSet(messages));
    }

    private void testWriteToWithMessageSet(MessageSet set) {
        // do the write twice to ensure the message set is restored to its orginal state
        try {
            for (int i = 0; i < 2; i++) {
                File file = TestUtils.tempFile();
                FileChannel channel = new RandomAccessFile(file, "rw").getChannel();

                Integer written = set.writeTo(channel, 0L, 1024);
                Assert.assertEquals("Expect to write the number of bytes in the set.", set.sizeInBytes(), written);
                // TODO: 2017/3/27 fileMessage未写完
//                FileMessageSet newSet = new FileMessageSet(file, channel);
//                checkEquals(set.iterator(), newSet.iterator());
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
