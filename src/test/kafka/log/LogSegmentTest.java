package kafka.log;/**
 * Created by zhoulf on 2017/3/30.
 */

import com.google.common.collect.Lists;
import kafka.message.*;
import kafka.server.FetchDataInfo;
import kafka.utils.Time;
import kafka.utils.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author
 * @create 2017-03-30 37 10
 **/
public class LogSegmentTest {
    List<LogSegment> segments = Lists.newArrayList();

    /* create a segment with the given base offset */
    public LogSegment createSegment(Long offset) throws IOException {
        File msFile = TestUtils.tempFile();
        FileMessageSet ms = new FileMessageSet(msFile);
        File idxFile = TestUtils.tempFile();
        idxFile.delete();
        OffsetIndex idx = new OffsetIndex(idxFile, offset, 1000);
        LogSegment seg = new LogSegment(ms, idx, offset, 10, 0L, Time.get());
        segments.add(seg);
        return seg;
    }

    /* create a ByteBufferMessageSet for the given messages starting from the given offset */
    public ByteBufferMessageSet messages(Long offset, List<String> messages) {
        return new ByteBufferMessageSet(CompressionCodec.NoCompressionCodec,
                new AtomicLong(offset),
                messages.stream().map((s) -> new Message(s.getBytes())).collect(Collectors.toList()));
    }

    @After
    public void teardown() {
        for (LogSegment seg : segments) {
            seg.index.delete();
            seg.log.delete();
        }
    }

    /**
     * A read on an empty log segment should return null
     */
    @Test
    public void testReadOnEmptySegment() throws Exception {
        LogSegment seg = createSegment(40L);
        FetchDataInfo read = seg.read(40L, Optional.empty(), 300);
        Assert.assertNull("Read beyond the last offset in the segment should be null", read);
    }

    /**
     * Reading from before the first offset in the segment should return messages
     * beginning with the first message in the segment
     */
    @Test
    public void testReadBeforeFirstOffset() throws Exception {
        LogSegment seg = createSegment(40L);
        ByteBufferMessageSet ms = messages(50L, Lists.newArrayList("hello", "there", "little", "bee"));
        seg.append(50L, ms);
        MessageSet read = seg.read(41L, Optional.empty(), 300).messageSet;
        TestUtils.checkEquals(ms.iterator(), read.iterator());
    }

    /**
     * If we set the startOffset and maxOffset for the read to be the same value
     * we should get only the first message in the log
     */
    @Test
    public void testMaxOffset() throws Exception {
        Long baseOffset = 50L;
        LogSegment segment = createSegment(baseOffset);
        ByteBufferMessageSet messageSet = messages(baseOffset, Lists.newArrayList("hello", "there", "beautiful"));
        segment.append(baseOffset, messageSet);

        validate(50L, messageSet, segment);
        validate(51L, messageSet, segment);
        validate(52L, messageSet, segment);
    }

    void validate(Long offset, ByteBufferMessageSet messageSet, LogSegment segment) {
        List<MessageAndOffset> list = Lists.newArrayList();
        for (MessageAndOffset m : messageSet) {
            if (m.offset == offset) {
                list.add(m);
            }
        }
        TestUtils.checkEquals(list.iterator(), segment.read(offset, Optional.of(offset + 1), 1024).messageSet.iterator());
    }

    /**
     * If we read from an offset beyond the last offset in the segment we should get null
     */
    @Test
    public void testReadAfterLast() throws Exception {
        LogSegment seg = createSegment(40L);
        ByteBufferMessageSet ms = messages(50L, Lists.newArrayList("hello", "there"));
        seg.append(50L, ms);
        FetchDataInfo read = seg.read(52L, Optional.empty(), 200);
        Assert.assertNull("Read beyond the last offset in the segment should give null", read);
    }

    /**
     * If we read from an offset which doesn't exist we should get a message set beginning
     * with the least offset greater than the given startOffset.
     */
    @Test
    public void testReadFromGap() throws Exception {
        LogSegment seg = createSegment(40L);
        ByteBufferMessageSet ms = messages(50L, Lists.newArrayList("hello", "there"));
        seg.append(50L, ms);
        ByteBufferMessageSet ms2 = messages(60L, Lists.newArrayList("alpha", "beta"));
        seg.append(60L, ms2);
        FetchDataInfo read = seg.read(55L, Optional.empty(), 200);
        TestUtils.checkEquals(ms2.iterator(), read.messageSet.iterator());
    }

    /**
     * In a loop append two messages then truncate off the second of those messages and check that we can read
     * the first but not the second message.
     */
    @Test
    public void testTruncate() throws IOException {
        LogSegment seg = createSegment(40L);
        Long offset = 40L;
        for (int i = 0; i < 30; i++) {
            ByteBufferMessageSet ms1 = messages(offset, Lists.newArrayList("hello"));
            seg.append(offset, ms1);
            ByteBufferMessageSet ms2 = messages(offset + 1, Lists.newArrayList("hello"));
            seg.append(offset + 1, ms2);
            // check that we can read back both messages;
            FetchDataInfo read = seg.read(offset, Optional.empty(), 10000);
            TestUtils.checkEquals(Lists.newArrayList(ms1.head(), ms2.head()).iterator(), read.messageSet.iterator());
            // now truncate off the last message;
            seg.truncateTo(offset + 1);
            FetchDataInfo read2 = seg.read(offset, Optional.empty(), 10000);
            Assert.assertEquals(1, read2.messageSet.toMessageAndOffsetList().size());
            Assert.assertEquals(ms1.head(), read2.messageSet.head());
            offset += 1;
        }
    }

    /**
     * Test truncating the whole segment, and check that we can reappend with the original offset.
     */
    @Test
    public void testTruncateFull() throws Exception {
        // test the case where we fully truncate the log;
        LogSegment seg = createSegment(40L);
        seg.append(40L, messages(40L, Lists.newArrayList("hello", "there")));
        seg.truncateTo(0L);
        Assert.assertNull("Segment should be empty.", seg.read(0L, Optional.empty(), 1024));
        seg.append(40L, messages(40L, Lists.newArrayList("hello", "there")));
    }

    /**
     * Test that offsets are assigned sequentially and that the nextOffset variable is incremented
     */
    @Test
    public void testNextOffsetCalculation() throws Exception {
        LogSegment seg = createSegment(40L);
        Assert.assertEquals(new Long(40L), seg.nextOffset());
        seg.append(50L, messages(50L, Lists.newArrayList("hello", "there", "you")));
        Assert.assertEquals(new Long(53L), seg.nextOffset());
    }

    /**
     * Test that we can change the file suffixes for the log and index files
     */
    @Test
    public void testChangeFileSuffixes() throws Exception {
        LogSegment seg = createSegment(40L);
        File logFile = seg.log.file;
        File indexFile = seg.index.file;
        // TODO: 2017/4/1 windows fails
        seg.changeFileSuffixes("", ".deleted");
        Assert.assertEquals(logFile.getAbsolutePath() + ".deleted", seg.log.file.getAbsolutePath());
        Assert.assertEquals(indexFile.getAbsolutePath() + ".deleted", seg.index.file.getAbsolutePath());
        Assert.assertTrue(seg.log.file.exists());
        Assert.assertTrue(seg.index.file.exists());
    }

    /**
     * Create a segment with some data and an index. Then corrupt the index,
     * and recover the segment, the entries should all be readable.
     */
    @Test
    public void testRecoveryFixesCorruptIndex() throws Exception {
        LogSegment seg = createSegment(0L);
        for (long i = 0; i < 100; i++)
            seg.append(i, messages(i, Lists.newArrayList("" + i)));
        File indexFile = seg.index.file;
        TestUtils.writeNonsenseToFile(indexFile, 5L, (int) indexFile.length());
        seg.recover(64 * 1024);
        for (long i = 0; i < 100; i++)
            Assert.assertEquals(new Long(i), seg.read(i, Optional.of(i + 1), 1024).messageSet.head().offset);
    }

    /**
     * Randomly corrupt a log a number of times and attempt recovery.
     */
    @Test
    public void testRecoveryWithCorruptMessage() throws Exception {
        Integer messagesAppended = 20;
        for (int iteration = 0; iteration < 10; iteration++) {
            LogSegment seg = createSegment(0L);
            for (int i = 0; i < messagesAppended; i++)
                seg.append(new Long(i), messages(new Long(i), Lists.newArrayList("" + i)));
            Integer offsetToBeginCorruption = TestUtils.random.nextInt(messagesAppended);
            // start corrupting somewhere in the middle of the chosen record all the way to the end;
            Integer position = seg.log.searchFor(offsetToBeginCorruption.longValue(), 0).position + TestUtils.random.nextInt(15);
            TestUtils.writeNonsenseToFile(seg.log.file, position.longValue(), (int) seg.log.file.length() - position);
            seg.recover(64 * 1024);
//            Assert.assertEquals("Should have truncated off bad messages.", (0until offsetToBeginCorruption).toList, seg.log.map(_.offset).toList)
            Assert.assertEquals("Should have truncated off bad messages.",
                    Stream.iterate(0L, n -> n + 1).limit(offsetToBeginCorruption).collect(Collectors.toList()),
                    seg.log.toMessageAndOffsetList().stream().map((m) -> m.offset).collect(Collectors.toList()));
            // TODO: 2017/4/1 windows fail segment.delete()
//            seg.delete();
        }
    }
}
