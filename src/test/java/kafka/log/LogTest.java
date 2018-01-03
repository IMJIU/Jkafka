package kafka.log;

import com.google.common.collect.Lists;
import kafka.common.MessageSetSizeTooLargeException;
import kafka.common.MessageSizeTooLargeException;
import kafka.common.OffsetOutOfRangeException;
import kafka.message.*;
import kafka.server.FetchDataInfo;
import kafka.utils.*;
import kafka.server.KafkaConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by Administrator on 2017/4/3.
 */
public class LogTest {
    Logging logging = Logging.getLogger(LogTest.class.getName());
    File logDir = null;
    MockTime time = new MockTime(0L);
    KafkaConfig config = null;
    LogConfig logConfig = new LogConfig();

    @Before
    public void setUp() throws Exception {
        logDir = TestUtils.tempDir();
        Properties props = TestUtils.createBrokerConfig(0, -1, true);
        config = new KafkaConfig(props);
    }

    public LogConfig copy() {
        try {
            return logConfig.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return null;
    }

    @After
    public void tearDown() {
        Utils.rm(logDir);
    }

    public void createEmptyLogs(File dir, Integer... offsets) throws IOException {
        for (Integer offset : offsets) {
            Log.logFilename(dir, offset.longValue()).createNewFile();
            Log.indexFilename(dir, offset.longValue()).createNewFile();
        }
    }

    /**
     * Test that we can open and append to an empty log
     * 往空日志里，增加一条msg
     */
    @Test
    public void testLoadEmptyLog() throws IOException {
        createEmptyLogs(logDir, 0);
        Log log = new Log(logDir, logConfig, 0L, time.scheduler, time);
        log.append(TestUtils.singleMessageSet("test".getBytes()));
    }

    /**
     * This test covers an odd case where we have a gap in the offsets that falls at the end of a log segment.
     * Specifically we create a log where the last message in the first segment has offset 0. If we
     * then read offset 1, we should expect this read to come from the second segment, even though the
     * first segment has the greatest lower bound on the offset.
     * 一直插入，直到roll成两个segement 。
     * 对之前的segment进行truncate 不影响log read()
     */
    @Test
    public void testReadAtLogGap() throws IOException {
        LogConfig copy = copy();
        copy.segmentSize = 300;
        Log log = new Log(logDir, copy, 0L, time.scheduler, time);

        // keep appending until we have two segments with only a single message in the second segment;
        while (log.numberOfSegments() == 1)
            log.append(new ByteBufferMessageSet(CompressionCodec.NoCompressionCodec, new Message("42".getBytes())));

        // now manually truncate off all but one message from the first segment to create a gap in the messages;
        log.logSegments().stream().findFirst().get().truncateTo(1L);
        System.out.println(log.logEndOffset() - 1);
        System.out.println(log.read(1L, 200, Optional.empty()).messageSet.head().offset);
        Assert.assertEquals("A read should now return the last message in the log", new Long(log.logEndOffset() - 1), log.read(1L, 200, Optional.empty()).messageSet.head().offset);
    }

    /**
     * 追加空信息
     * 消息size为-1
     *
     * @throws IOException
     */
    @Test
    public void testAppendMessageWithNullPayload() throws IOException {
        Log log = new Log(logDir, new LogConfig(), 0L, time.scheduler, time);
        byte[] b = null;
        log.append(new ByteBufferMessageSet(new Message(b)));
        MessageSet messageSet = log.read(0L, 4096, Optional.empty()).messageSet;
        Assert.assertEquals(new Long(0), messageSet.head().offset);//第一条信息offset为0
        Assert.assertTrue("Message payload should be null.", messageSet.head().message.isNull());//b本来就是null。content为null,size为-1
    }

    /**
     * Test reading at the boundary of the log, specifically
     * - reading from the logEndOffset should give an empty message set
     * - reading beyond the log end offset should throw an OffsetOutOfRangeException
     * 读取边界值（初始1024offset)。   if (startOffset > next || entry == null) 判断，然后抛异常
     * 在segment内，但正好等于初始1024offset返回空消息
     * 小于初始1024offset 报异常
     * 大于最大nextOffset 报异常
     */
    @Test
    public void testReadOutOfRange() throws IOException {
        createEmptyLogs(logDir, 1024);
        Log log = new Log(logDir, getLogConfig(1024), 0L, time.scheduler, time);
        Assert.assertEquals("Reading just beyond end of log should produce 0 byte read.", new Integer(0), log.read(1024L, 1000).messageSet.sizeInBytes());
        try {
            log.read(0L, 1024);
            Assert.fail("Expected exception on invalid read.");
        } catch (OffsetOutOfRangeException e) {
            System.out.println("This is good.");
        }
        try {
            log.read(1025L, 1000);
            Assert.fail("Expected exception on invalid read.");
        } catch (OffsetOutOfRangeException e) {
            System.out.println("This is good.");
        }
    }

    /**
     * This test case appends a bunch of messages and checks that we can read them all back using sequential offsets.
     * 顺序连续offset校验，0,2,4..100 校验offset和value是否匹配
     */
    @Test
    public void testAppendAndReadWithSequentialOffsets() throws IOException {
        LogConfig copy = copy();
        copy.segmentSize = 71;
        Log log = new Log(logDir, copy, 0L, time.scheduler, time);
        //50个 0,2,4...100
        List<Message> messages = Stream.iterate(0, n -> n + 2).limit(100).map(id -> new Message(id.toString().getBytes())).collect(Collectors.toList());

        for (int i = 0; i < messages.size(); i++)
            log.append(new ByteBufferMessageSet(CompressionCodec.NoCompressionCodec, messages.get(i)));
        for (long i = 0; i < messages.size(); i++) {
            MessageAndOffset read = log.read(i, 100, Optional.of(i + 1L)).messageSet.head();
            Assert.assertEquals("Offset read should match order appended.", new Long(i), read.offset);
            Assert.assertEquals("Message should match appended.", messages.get((int) i), read.message);
        }
        Assert.assertEquals("Reading beyond the last message returns nothing.", 0, log.read((long) messages.size(), 100, Optional.empty()).messageSet.toMessageAndOffsetList().size());
    }

    /**
     * This test appends a bunch of messages with non-sequential offsets and checks that we can read the correct message
     * from any offset less than the logEndOffset including offsets not appended.
     * 非连续offset 校验
     */
    @Test
    public void testAppendAndReadWithNonSequentialOffsets() throws IOException {
        Log log = new Log(logDir, getLogConfig(71), 0L, time.scheduler, time);
        List<Integer> messageIds = Stream.iterate(0, n -> n + 1).limit(50).collect(Collectors.toList());
        messageIds.addAll(Stream.iterate(50, n -> n + 7).limit(200).collect(Collectors.toList()));
        List<Message> messages = messageIds.stream().map(id -> new Message(id.toString().getBytes())).collect(Collectors.toList());

        // now test the case that we give the offsets and use non-sequential offsets;
        for (int i = 0; i < messages.size(); i++) {
            log.append(new ByteBufferMessageSet(CompressionCodec.NoCompressionCodec, new AtomicLong(messageIds.get(i)), messages.get(i)), false);
        }
        for (int i = 50; i < messageIds.get(messageIds.size() - 1); i++) {
            Integer idx = null;
            for (int j = 0; j < messageIds.size(); j++) {
                if (messageIds.get(j) >= i) {
                    idx = j;
                    break;
                }
            }
            MessageAndOffset read = log.read((long) i, 100, Optional.empty()).messageSet.head();
            Assert.assertEquals("Offset read should match message id.", new Long(messageIds.get(idx)), read.offset);
            Assert.assertEquals("Message should match appended.", messages.get(idx), read.message);
        }
    }


    /**
     * Test reads at offsets that fall within compressed message set boundaries.
     * 压缩校验
     */
    @Test
    public void testCompressedMessages() throws IOException {
        //segmentSize=100 正好一条消息一个segment
    /* this log should roll after every messageset */
        Log log = new Log(logDir, getLogConfig(100), 0L, time.scheduler, time);

    /* append 2 compressed message sets, each with two messages giving offsets 0, 1, 2, 3 */
        //offset[0,1]1
        log.append(new ByteBufferMessageSet(CompressionCodec.GZIPCompressionCodec, new Message("hello".getBytes()), new Message("there".getBytes())));
        //offset[0,1]1[2,3]3
        log.append(new ByteBufferMessageSet(CompressionCodec.GZIPCompressionCodec, new Message("alpha".getBytes()), new Message("beta".getBytes())));

    /* we should always get the first message in the compressed set when reading any offset in the set */
        Assert.assertEquals("Read at offset 0 should produce 0", new Long(0), read(log, 0L).head().offset);
        Assert.assertEquals("Read at offset 1 should produce 0", new Long(0), read(log, 1L).head().offset);
        Assert.assertEquals("Read at offset 2 should produce 2", new Long(2), read(log, 2L).head().offset);
        Assert.assertEquals("Read at offset 3 should produce 2", new Long(2), read(log, 3L).head().offset);
    }

    public ByteBufferMessageSet read(Log log, Long offset) throws IOException {
        FetchDataInfo fetchDataInfo = log.read(offset, 4096);
        fetchDataInfo.messageSet.printAll();
        ByteBufferMessageSet result = ByteBufferMessageSet.decompress(fetchDataInfo.messageSet.head().message);
        return result;
    }

    /**
     * Test garbage collecting old segments
     */
    @Test
    public void testThatGarbageCollectingSegmentsDoesntChangeOffset() throws IOException {
        for (Integer messagesToAppend : Lists.newArrayList(0, 1, 25)) {
            logDir.mkdirs();
            // first test a log segment starting at 0;
            Log log = new Log(logDir, getLogConfig(100), 0L, time.scheduler, time);
            for (int i = 0; i < messagesToAppend; i++)
                log.append(TestUtils.singleMessageSet(("" + i).getBytes()));

            Long currOffset = log.logEndOffset();
            Assert.assertEquals(currOffset, new Long(messagesToAppend));

            // time goes by; the log file is deleted;
            log.deleteOldSegments((s) -> true);

            Assert.assertEquals("Deleting segments shouldn't have changed the logEndOffset", currOffset, log.logEndOffset());
            Assert.assertEquals("We should still have one segment left", new Integer(1), log.numberOfSegments());
            Assert.assertEquals("Further collection shouldn't delete anything", new Integer(0), log.deleteOldSegments((s) -> true));
            Assert.assertEquals("Still no change in the logEndOffset", currOffset, log.logEndOffset());
            Assert.assertEquals("Should still be able to append and should get the logEndOffset assigned to the new append",
                    currOffset,log.append(TestUtils.singleMessageSet("hello".toString().getBytes())).firstOffset);

            // cleanup the log;
            log.delete();
        }
    }

    /**
     * MessageSet size shouldn't exceed the config.segmentSize, check that it is properly enforced by
     * appending a message set larger than the config.segmentSize setting and checking that an exception is thrown.
     * 超过segmentSize 是否报错
     */
    @Test
    public void testMessageSetSizeCheck() throws IOException {
        ByteBufferMessageSet messageSet = new ByteBufferMessageSet(CompressionCodec.NoCompressionCodec, new Message("You".getBytes()), new Message("bethe".getBytes()));
        // append messages to log;
        Integer configSegmentSize = messageSet.sizeInBytes() - 1;
        Log log = new Log(logDir, getLogConfig(configSegmentSize), 0L, time.scheduler, time);
        try {
            log.append(messageSet);
            Assert.fail("message set should throw MessageSetSizeTooLargeException.");
        } catch (MessageSetSizeTooLargeException e) {
            System.out.println("this is good");
        }
    }

    /**
     * We have a max size limit on message appends, check that it is properly enforced by appending a message larger than the
     * setting and checking that an exception is thrown.
     * 单条信息超过maxMessageSize是否报错
     */
    @Test
    public void testMessageSizeCheck() throws IOException {
        ByteBufferMessageSet first = new ByteBufferMessageSet(CompressionCodec.NoCompressionCodec, new Message("You".getBytes()), new Message("bethe".getBytes()));
        ByteBufferMessageSet second = new ByteBufferMessageSet(CompressionCodec.NoCompressionCodec, new Message("change".getBytes()));
        System.out.println("first:"+first.sizeInBytes());
        System.out.println("second:"+second.sizeInBytes());
        // append messages to log;
        LogConfig config = copy();
        config.maxMessageSize = second.sizeInBytes() - 1;
        Log log = new Log(logDir, config, 0L, time.scheduler, time);

        // should be able to append the small message;
        log.append(first);
        try {
            log.append(second);
            Assert.fail("Second message set should throw MessageSizeTooLargeException.");
        } catch (MessageSizeTooLargeException e) {
            System.out.println("this is good");
        }
    }

    /**
     * Append a bunch of messages to a log and then re-open it both with and without recovery and check that the log re-initializes correctly.
     */
    @Test
    public void testLogRecoversToCorrectOffset() throws IOException {
        Integer numMessages = 100;
        Integer messageSize = 100;
        Integer segmentSize = 7 * messageSize;
        Integer indexInterval = 3 * messageSize;
        LogConfig config = getLogConfig(segmentSize);
        config.indexInterval = indexInterval;
        config.maxIndexSize = 4096;
        Log log = new Log(logDir, config, 0L, time.scheduler, time);
        for (int i = 0; i < numMessages; i++)
            log.append(TestUtils.singleMessageSet(TestUtils.randomBytes(messageSize)));
        Assert.assertEquals(String.format("After appending %d messages to an empty log, the log end offset should be %d", numMessages, numMessages), new Long(numMessages), log.logEndOffset());
        Long lastIndexOffset = log.activeSegment().index.lastOffset;
        Integer numIndexEntries = log.activeSegment().index.entries();
        Long lastOffset = log.logEndOffset();
        log.close();

        log = new Log(logDir, config, lastOffset, time.scheduler, time);
        Assert.assertEquals(String.format("Should have %d messages when log is reopened w/o recovery", numMessages), new Long(numMessages), log.logEndOffset());
        Assert.assertEquals("Should have same last index offset as before.", lastIndexOffset, log.activeSegment().index.lastOffset);
        Assert.assertEquals("Should have same number of index entries as before.", numIndexEntries, log.activeSegment().index.entries());
        log.close();

        // test recovery case;
        log = new Log(logDir, config, 0L, time.scheduler, time);
        Assert.assertEquals(String.format("Should have %d messages when log is reopened with recovery", numMessages), new Long(numMessages), log.logEndOffset());
        Assert.assertEquals("Should have same last index offset as before.", lastIndexOffset, log.activeSegment().index.lastOffset);
        Assert.assertEquals("Should have same number of index entries as before.", numIndexEntries, log.activeSegment().index.entries());
        log.close();
    }

    /**
     * Test that if we manually delete an index segment it is rebuilt when the log is re-opened
     */
    @Test
    public void testIndexRebuild() throws IOException {
        // publish the messages and close the log;
        Integer numMessages = 200;
        LogConfig config = getLogConfig(200);
        config.indexInterval = 1;
        Log log = new Log(logDir, config, 0L, time.scheduler, time);
        for (int i = 0; i < numMessages; i++)
            log.append(TestUtils.singleMessageSet(TestUtils.randomBytes(10)));
        List<File> indexFiles = log.logSegments().stream().map((s) -> s.index.file).collect(Collectors.toList());
        log.close();

        // delete all the index files;
        indexFiles.forEach(f -> f.delete());

        // reopen the log;
        log = new Log(logDir, config, 0L, time.scheduler, time);
        Assert.assertEquals(String.format("Should have %d messages when log is reopened", numMessages), new Long(numMessages), log.logEndOffset());
        for (int i = 0; i < numMessages; i++)
            Assert.assertEquals(new Long(i), log.read((long) i, 100, Optional.empty()).messageSet.head().offset);
        log.close();
    }

    /**
     * Test the Log truncate operations
     */
    @Test
    public void testTruncateTo() throws IOException {
        ByteBufferMessageSet set = TestUtils.singleMessageSet("test".getBytes());
        Integer setSize = set.sizeInBytes();
        Integer msgPerSeg = 10;
        Integer segmentSize = msgPerSeg * setSize;  // each segment will be 10 messages;

        // create a log;
        Log log = new Log(logDir, getLogConfig(segmentSize), 0L, time.scheduler, time);
        Assert.assertEquals("There should be exactly 1 segment.", new Integer(1), log.numberOfSegments());

        for (int i = 1; i <= msgPerSeg; i++)
            log.append(set);

        Assert.assertEquals("There should be exactly 1 segments.", new Integer(1), log.numberOfSegments());
        Assert.assertEquals("Log end offset should be equal to number of messages", new Long(msgPerSeg), log.logEndOffset());

        Long lastOffset = log.logEndOffset();
        Long size = log.size();
        log.truncateTo(log.logEndOffset()); // keep the entire log;
        Assert.assertEquals("Should not change offset", lastOffset, log.logEndOffset());
        Assert.assertEquals("Should not change log size", size, log.size());
        log.truncateTo(log.logEndOffset() + 1); // try to truncate beyond lastOffset;
        Assert.assertEquals("Should not change offset but should log error", lastOffset, log.logEndOffset());
        Assert.assertEquals("Should not change log size", size, log.size());
        log.truncateTo(msgPerSeg / 2L); // truncate somewhere in between;
        Assert.assertEquals("Should change offset", log.logEndOffset(), new Long(msgPerSeg / 2));
        Assert.assertTrue("Should change log size", log.size() < size);
        log.truncateTo(0L); // truncate the entire log;
        Assert.assertEquals("Should change offset", new Long(0), log.logEndOffset());
        Assert.assertEquals("Should change log size", new Long(0), log.size());

        for (int i = 1; i <= msgPerSeg; i++)
            log.append(set);

        Assert.assertEquals("Should be back to original offset", log.logEndOffset(), lastOffset);
        Assert.assertEquals("Should be back to original size", log.size(), size);
        log.truncateFullyAndStartAt(log.logEndOffset() - (msgPerSeg - 1));
        Assert.assertEquals("Should change offset", log.logEndOffset(), new Long(lastOffset - (msgPerSeg - 1)));
        Assert.assertEquals("Should change log size", log.size(), new Long(0));

        for (int i = 1; i <= msgPerSeg; i++)
            log.append(set);

        Assert.assertTrue("Should be ahead of to original offset", log.logEndOffset() > msgPerSeg);
        Assert.assertEquals("log size should be same as before", size, log.size());
        log.truncateTo(0L); // truncate before first start offset in the log;
        Assert.assertEquals("Should change offset", new Long(0), log.logEndOffset());
        Assert.assertEquals("Should change log size", log.size(), new Long(0));
    }

    /**
     * Verify that when we truncate a log the index of the last segment is resized to the max index size to allow more appends
     */
    @Test
    public void testIndexResizingAtTruncation() throws IOException {
        ByteBufferMessageSet set = TestUtils.singleMessageSet("test".getBytes());
        Integer setSize = set.sizeInBytes();
        Integer msgPerSeg = 10;
        Integer segmentSize = msgPerSeg * setSize;  // each segment will be 10 messages;
        Log log = new Log(logDir, getLogConfig(segmentSize), 0L, time.scheduler, time);
        Assert.assertEquals("There should be exactly 1 segment.", new Integer(1), log.numberOfSegments());
        for (int i = 1; i <= msgPerSeg; i++)
            log.append(set);
        Assert.assertEquals("There should be exactly 1 segment.", new Integer(1), log.numberOfSegments());
        for (int i = 1; i <= msgPerSeg; i++)
            log.append(set);
        Assert.assertEquals("There should be exactly 2 segment.", new Integer(2), log.numberOfSegments());
        //为什么为0，因为之前就没到indexInterval 插入就为0
        Assert.assertEquals("The index of the first segment should be trimmed to empty", new Integer(0), log.logSegments().stream().findFirst().get().index.maxEntries);
        log.truncateTo(0L);
        Assert.assertEquals("There should be exactly 1 segment.", new Integer(1), log.numberOfSegments());
        Assert.assertEquals("The index of segment 1 should be resized to maxIndexSize", new Long(log.config.maxIndexSize / 8), new Long(log.logSegments().stream().findFirst().get().index.maxEntries));
        for (int i = 1; i <= msgPerSeg; i++)
            log.append(set);
        Assert.assertEquals("There should be exactly 1 segment.", new Integer(1), log.numberOfSegments());
    }

    /**
     * When we open a log any index segments without an associated log segment should be deleted.
     */
    @Test
    public void testBogusIndexSegmentsAreRemoved() throws IOException {
        File bogusIndex1 = Log.indexFilename(logDir, 0L);
        File bogusIndex2 = Log.indexFilename(logDir, 5L);

        ByteBufferMessageSet set = TestUtils.singleMessageSet("test".getBytes());
        LogConfig config = getLogConfig(set.sizeInBytes() * 5);
        config.indexInterval = 1;
        config.maxIndexSize = 1000;
        Log log = new Log(logDir, config, 0L, time.scheduler, time);

        Assert.assertTrue("The first index file should have been replaced with a larger file", bogusIndex1.length() > 0);
        Assert.assertFalse("The second index file should have been deleted.", bogusIndex2.exists());

        // check that we can append to the log;
        for (int i = 0; i < 10; i++)
            log.append(set);

        log.delete();
    }

    /**
     * Verify that truncation works correctly after re-opening the log
     */
    @Test
    public void testReopenThenTruncate() throws IOException {
        ByteBufferMessageSet set = TestUtils.singleMessageSet("test".getBytes());
        LogConfig config = getLogConfig(set.sizeInBytes() * 5);
        config.indexInterval = 10000;
        config.maxIndexSize = 1000;

        // create a log;
        Log log = new Log(logDir, config, 0L, time.scheduler, time);

        // add enough messages to roll over several segments then close and re-open and attempt to truncate;
        for (int i = 0; i < 100; i++)
            log.append(set);
        log.close();
        log = new Log(logDir, config, 0L, time.scheduler, time);
        Assert.assertEquals(new Integer(20), log.numberOfSegments());
        log.truncateTo(3L);
        Assert.assertEquals("All but one segment should be deleted.", new Integer(1), log.numberOfSegments());
        Assert.assertEquals("Log end offset should be 3.", new Long(3), log.logEndOffset());
    }

    /**
     * Test that deleted files are deleted after the appropriate time.
     */
    @Test
    public void testAsyncDelete() throws IOException {
        ByteBufferMessageSet set = TestUtils.singleMessageSet("test".getBytes());
        Long asyncDeleteMs = 1000L;
        LogConfig config = getLogConfig(set.sizeInBytes() * 5);
        config.indexInterval = 10000;
        config.maxIndexSize = 1000;
        config.fileDeleteDelayMs = asyncDeleteMs;
        Log log = new Log(logDir, config, 0L, time.scheduler, time);

        // append some messages to create some segments;
        for (int i = 0; i < 100; i++)
            log.append(set);

        // files should be renamed;
        List<LogSegment> segments = Lists.newArrayList(log.logSegments());
        List<File> oldFiles = segments.stream().map(s -> s.log.file).collect(Collectors.toList());
        oldFiles.addAll(segments.stream().map(s -> s.index.file).collect(Collectors.toList()));
        log.deleteOldSegments(s -> true);

        Assert.assertEquals("Only one segment should remain.", new Integer(1), log.numberOfSegments());
        Assert.assertTrue("All log and index files should end in .deleted", segments.stream().allMatch(s -> s.log.file.getName().endsWith(Log.DeletedFileSuffix)));
        Assert.assertTrue("All log and index files should end in .deleted", segments.stream().allMatch(s -> s.index.file.getName().endsWith(Log.DeletedFileSuffix)));
        Assert.assertTrue("The .deleted files should still be there.", segments.stream().allMatch(s -> s.log.file.exists()) && segments.stream().allMatch(s -> s.index.file.exists()));
        Assert.assertTrue("The original file should be gone.", oldFiles.stream().allMatch(s -> !s.exists()));

        // when enough time passes the files should be deleted;
        List<File> deletedFiles = segments.stream().map(s -> s.log.file).collect(Collectors.toList());
        deletedFiles.addAll(segments.stream().map(s -> s.index.file).collect(Collectors.toList()));
        time.sleep(asyncDeleteMs + 1);
        Assert.assertTrue("Files should all be gone.", deletedFiles.stream().allMatch(s -> !s.exists()));
    }

    /**
     * Any files ending in .deleted should be removed when the log is re-opened.
     */
    @Test
    public void testOpenDeletesObsoleteFiles() throws IOException {
        ByteBufferMessageSet set = TestUtils.singleMessageSet("test".getBytes());
        LogConfig config = getLogConfig(set.sizeInBytes() * 5);
        config.maxIndexSize = 1000;
        Log log = new Log(logDir, config, 0L, time.scheduler, time);

        // append some messages to create some segments;
        for (int i = 0; i < 100; i++)
            log.append(set);

        log.deleteOldSegments(s -> true);
        log.close();
        log = new Log(logDir, config, 0L, time.scheduler, time);
        Assert.assertEquals("The deleted segments should be gone.", new Integer(1), log.numberOfSegments());
    }


    @Test
    public void testCorruptLog() throws IOException {
        // append some messages to create some segments;
        LogConfig config = getLogConfig(1000);
        config.indexInterval = 1;
        config.maxMessageSize = 64 * 1024;
        ByteBufferMessageSet set = TestUtils.singleMessageSet("test".getBytes());
        Long recoveryPoint = 50L;
        for (int iteration = 0; iteration < 50; iteration++) {
            // create a log and write some messages to it;
            logDir.mkdirs();
            Log log = new Log(logDir, config, 0L, time.scheduler, time);
            Integer numMessages = 50 + TestUtils.random.nextInt(50);
            for (int i = 0; i < numMessages; i++)
                log.append(set);
            List<MessageAndOffset> messages = log.logSegments().stream().flatMap(s -> s.log.toMessageAndOffsetList().stream()).collect(Collectors.toList());
            log.close();

            // corrupt index and log by appending random bytes;
            TestUtils.appendNonsenseToFile(log.activeSegment().index.file, TestUtils.random.nextInt(1024) + 1);
            TestUtils.appendNonsenseToFile(log.activeSegment().log.file, TestUtils.random.nextInt(1024) + 1);

            // attempt recovery;
            log = new Log(logDir, config, recoveryPoint, time.scheduler, time);
            Assert.assertEquals(new Long(numMessages), log.logEndOffset());
            Assert.assertEquals("Messages in the log after recovery should be the same.", messages, log.logSegments().stream().flatMap(s -> s.log.toMessageAndOffsetList().stream()).collect(Collectors.toList()));
            Utils.rm(logDir);
        }
    }

    @Test
    public void testCleanShutdownFile() throws IOException {
        // append some messages to create some segments;
        LogConfig config = getLogConfig(1000);
        config.indexInterval = 1;
        config.maxIndexSize = 64 * 1024;
        ByteBufferMessageSet set = TestUtils.singleMessageSet("test".getBytes());
        File parentLogDir = logDir.getParentFile();
        Assert.assertTrue("Data directory %s must exist", parentLogDir.isDirectory());
        File cleanShutdownFile = new File(parentLogDir, Log.CleanShutdownFile);
        cleanShutdownFile.createNewFile();
        Assert.assertTrue(".kafka_cleanshutdown must exist", cleanShutdownFile.exists());
        Long recoveryPoint;
        // create a log and write some messages to it;
        Log log = new Log(logDir, config, 0L, time.scheduler, time);
        for (int i = 0; i < 100; i++)
            log.append(set);
        log.close();

        // check if recovery was attempted. Even if the recovery point is 0L, recovery should not be attempted as the
        // clean shutdown file exists.;
        recoveryPoint = log.logEndOffset();
        log = new Log(logDir, config, 0L, time.scheduler, time);
        Assert.assertEquals(recoveryPoint, log.logEndOffset());
        cleanShutdownFile.delete();
    }

    private LogConfig getLogConfig(Integer segmentSize) {
        LogConfig copy = copy();
        copy.segmentSize = segmentSize;
        return copy;
    }

    /**
     * Tests for time based log roll. This test appends messages then changes the time
     * using the mock clock to force the log to roll and checks the number of segments.
     */
    @Test
    public void testTimeBasedLogRoll() throws CloneNotSupportedException, IOException {
        ByteBufferMessageSet set = TestUtils.singleMessageSet("test".getBytes());
        LogConfig config = logConfig.clone();
        config.segmentMs = 1 * 60 * 60L;
        // create a log;
        Log log = new Log(logDir, config, 0L, time.scheduler, time);
        Assert.assertEquals("Log begins with a single empty segment.", new Integer(1), log.numberOfSegments());
        time.sleep(log.config.segmentMs + 1);
        log.append(set);
        Assert.assertEquals("Log doesn't roll if doing so creates an empty segment.", new Integer(1), log.numberOfSegments());

        log.append(set);
        Assert.assertEquals("Log rolls on this append since time has expired.", new Integer(2), log.numberOfSegments());

        for (int numSegments = 3; numSegments < 5; numSegments++) {
            time.sleep(log.config.segmentMs + 1);
            log.append(set);
            Assert.assertEquals("Changing time beyond rollMs and appending should create a new segment.", new Integer(numSegments), log.numberOfSegments());
        }

        Integer numSegments = log.numberOfSegments();
        time.sleep(log.config.segmentMs + 1);
        log.append(new ByteBufferMessageSet());
        Assert.assertEquals("Appending an empty message set should not roll log even if succient time has passed.", numSegments, log.numberOfSegments());
    }

    /**
     * Test for jitter s for time based log roll. This test appends messages then changes the time
     * using the mock clock to force the log to roll and checks the number of segments.
     */
    @Test
    public void testTimeBasedLogRollJitter() throws Exception {
        ByteBufferMessageSet set = TestUtils.singleMessageSet("test".getBytes());
        Long maxJitter = 20 * 60L;
        LogConfig config = logConfig.clone();
        config.segmentMs = 1 * 60 * 60L;
        // create a log;
        Log log = new Log(logDir, config, 0L, time.scheduler, time);
        Assert.assertEquals("Log begins with a single empty segment.", new Integer(1), log.numberOfSegments());
        log.append(set);

        time.sleep(log.config.segmentMs - maxJitter);
        log.append(set);
        Assert.assertEquals("Log does not roll on this append because it occurs earlier than max jitter", new Integer(1), log.numberOfSegments());
        time.sleep(maxJitter - log.activeSegment().rollJitterMs + 1);
        log.append(set);
        Assert.assertEquals("Log should roll after segmentMs adjusted by random jitter", new Integer(2), log.numberOfSegments());
    }

    /**
     * Test that appending more than the maximum segment size rolls the log
     */
    @Test
    public void testSizeBasedLogRoll() throws IOException {
        ByteBufferMessageSet set = TestUtils.singleMessageSet("test".getBytes());
        Integer setSize = set.sizeInBytes();
        Integer msgPerSeg = 10;
        Integer segmentSize = msgPerSeg * (setSize - 1); // each segment will be 10 messages;
        LogConfig copy = copy();
        copy.segmentSize = segmentSize;
        // create a log;
        Log log = new Log(logDir, copy, 0L, time.scheduler, time);
        Assert.assertEquals("There should be exactly 1 segment.", new Integer(1), log.numberOfSegments());

        // segments expire in size;
        for (int i = -1; i <= msgPerSeg + 1; i++) {
            log.append(set);
        }
        Assert.assertEquals("There should be exactly 2 segments.", new Integer(2), log.numberOfSegments());
    }

    /**
     * Test that covers reads and writes on a multisegment log. This test appends a bunch of messages
     * and then reads them all back and checks that the message read and offset matches what was appended.
     */
    @Test
    public void testLogRolls() throws IOException {
    /* create a multipart log with 100 messages */
        Log log = new Log(logDir, getLogConfig(100), 0L, time.scheduler, time);
        Integer numMessages = 100;
        List<ByteBufferMessageSet> messageSets = Stream.iterate(0, n -> n++).limit(numMessages).map(i -> TestUtils.singleMessageSet(i.toString().getBytes())).collect(Collectors.toList());
        messageSets.forEach(m -> log.append(m));
        log.flush();

    /* do successive reads to ensure all our messages are there */
        Long offset = 0L;
        for (int i = 0; i < numMessages; i++) {
            MessageSet messages = log.read(offset, 1024 * 1024).messageSet;
            Assert.assertEquals("Offsets not equal", offset, messages.head().offset);
            Assert.assertEquals("Messages not equal at offset " + offset, messageSets.get(i).head().message, messages.head().message);
            offset = messages.head().offset + 1;
        }
        MessageSet lastRead = log.read(numMessages.longValue(), 1024 * 1024, Optional.of(numMessages + 1L)).messageSet;
        Assert.assertEquals("Should be no more messages", 0, lastRead.toMessageAndOffsetList().size());
        Assert.assertTrue("Log role should have forced flush", log.recoveryPoint >= log.activeSegment().baseOffset);
        logging.info("segment size:" + log.logSegments().size() + "");
        // check that rolling the log forced a flushed the log--the flush is asyn so retry in case of failure;
        TestUtils.retry(1000L, () -> Assert.assertTrue("Log role should have forced flush", log.recoveryPoint >= log.activeSegment().baseOffset));
    }

}
