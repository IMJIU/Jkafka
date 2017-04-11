package kafka.log;/**
 * Created by zhoulf on 2017/4/10.
 */

import kafka.common.LogCleaningAbortedException;
import kafka.func.ActionWithP;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.Throttler;
import kafka.utils.Utils;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * @author
 * @create 2017-04-10 43 17
 **/
public class CleanerTest {

    File dir = TestUtils.tempDir();
    LogConfig logConfig = LogConfig(segmentSize = 1024, maxIndexSize = 1024, compact = true);
    MockTime time = new MockTime();
    Throttler throttler = new Throttler(Double.MAX_VALUE, Long.MAX_VALUE, time);

    @After
    public void teardown() {
        Utils.rm(dir);
    }

    /**
     * Test simple log cleaning
     */
    @Test
    public void testCleanSegments() throws IOException {
        val cleaner = makeCleaner(Integer.MAX_VALUE);
        Log log = makeLog(dir,logConfig.copy(1024));

        // append messages to the log until we have four segments;
        while (log.numberOfSegments() < 4) ;
        log.append(message(log.logEndOffset().intValue(), log.logEndOffset().intValue()));
        val keysFound = keysInLog(log);
        Assert.assertEquals((0Luntil log.logEndOffset), keysFound);

        // pretend we have the following keys;
        val keys = immutable.ListSet(1, 3, 5, 7, 9);
        val map = new FakeOffsetMap(Integer.MAX_VALUE);
        keys.foreach(k = > map.put(key(k), Long.MAX_VALUE))

        // clean the log;
        cleaner.cleanSegments(log, log.logSegments.take(3).toSeq, map, 0L);
        val shouldRemain = keysInLog(log).filter(!keys.contains(_));
        Assert.assertEquals(shouldRemain, keysInLog(log));
    }

    @Test
    public void testCleaningWithDeletes() {
        val cleaner = makeCleaner(Integer.MAX_VALUE);
        val log = makeLog(config = logConfig.copy(segmentSize = 1024));

        // append messages with the keys 0 through N;
        while (log.numberOfSegments < 2) ;
        log.append(message(log.logEndOffset.toInt, log.logEndOffset.toInt));

        // delete all even keys between 0 and N;
        val leo = log.logEndOffset;
        for (key< -0 until leo.toInt by 2)
        log.append(deleteMessage(key));

        // append some new unique keys to pad out to a new active segment;
        while (log.numberOfSegments < 4) ;
        log.append(message(log.logEndOffset.toInt, log.logEndOffset.toInt));

        cleaner.clean(LogToClean(TopicAndPartition("test", 0), log, 0));
        val keys = keysInLog(log).toSet;
        assertTrue("None of the keys we deleted should still exist.",
                (0until leo.toInt by 2).forall(!keys.contains(_)))
    }

    /* extract all the keys from a log */
    public Iterable<Integer> keysInLog(Log log){
        return log.logSegments().stream().flatMap(s ->s.log.toMessageAndOffsetList().stream().filter(l->!l.message.isNull()).map(m ->Integer.parseInt(Utils.readString(m.message.key())))).collect(Collectors.toList());
    }

    public void abortCheckDone(TopicAndPartition topicAndPartition) {
        throw new LogCleaningAbortedException();
    }

    /**
     * Test that abortion during cleaning throws a LogCleaningAbortedException
     */
    @Test
    public void testCleanSegmentsWithAbort() {
        val cleaner = makeCleaner(Integer.MAX_VALUE, abortCheckDone);
        val log = makeLog(config = logConfig.copy(segmentSize = 1024));

        // append messages to the log until we have four segments;
        while (log.numberOfSegments < 4) ;
        log.append(message(log.logEndOffset.toInt, log.logEndOffset.toInt));

        val keys = keysInLog(log);
        val map = new FakeOffsetMap(Integer.MAX_VALUE);
        keys.foreach(k = > map.put(key(k), Long.MAX_VALUE))
        intercept<LogCleaningAbortedException> {
            cleaner.cleanSegments(log, log.logSegments.take(3).toSeq, map, 0L);
        }
    }

    /**
     * Validate the logic for grouping log segments together for cleaning
     */
    @Test
    public void testSegmentGrouping() {
        val cleaner = makeCleaner(Integer.MAX_VALUE);
        val log = makeLog(config = logConfig.copy(segmentSize = 300, indexInterval = 1));

        // append some messages to the log;
        var i = 0;
        while (log.numberOfSegments < 10) {
            log.append(TestUtils.singleMessageSet("hello".getBytes));
            i += 1;
        }

        // grouping by very large values should result in a single group with all the segments in it;
        var groups = cleaner.groupSegmentsBySize(log.logSegments, maxSize = Integer.MAX_VALUE, maxIndexSize = Integer.MAX_VALUE);
        Assert.assertEquals(1, groups.size);
        Assert.assertEquals(log.numberOfSegments, groups(0).size);
        checkSegmentOrder(groups);

        // grouping by very small values should result in all groups having one entry;
        groups = cleaner.groupSegmentsBySize(log.logSegments, maxSize = 1, maxIndexSize = Integer.MAX_VALUE);
        Assert.assertEquals(log.numberOfSegments, groups.size);
        assertTrue("All groups should be singletons.", groups.forall(_.size == 1))
        checkSegmentOrder(groups);
        groups = cleaner.groupSegmentsBySize(log.logSegments, maxSize = Integer.MAX_VALUE, maxIndexSize = 1);
        Assert.assertEquals(log.numberOfSegments, groups.size);
        assertTrue("All groups should be singletons.", groups.forall(_.size == 1))
        checkSegmentOrder(groups);

        val groupSize = 3;

        // check grouping by log size;
        val logSize = log.logSegments.take(groupSize).map(_.size).sum.toInt + 1;
        groups = cleaner.groupSegmentsBySize(log.logSegments, maxSize = logSize, maxIndexSize = Integer.MAX_VALUE);
        checkSegmentOrder(groups);
        assertTrue("All but the last group should be the target size.", groups.dropRight(1).forall(_.size == groupSize))

        // check grouping by index size;
        val indexSize = log.logSegments.take(groupSize).map(_.index.sizeInBytes()).sum + 1;
        groups = cleaner.groupSegmentsBySize(log.logSegments, maxSize = Integer.MAX_VALUE, maxIndexSize = indexSize);
        checkSegmentOrder(groups);
        assertTrue("All but the last group should be the target size.", groups.dropRight(1).forall(_.size == groupSize))
    }

    private public void checkSegmentOrder(Seq groups<Seq[LogSegment]>) {
        val offsets = groups.flatMap(_.map(_.baseOffset));
        Assert.assertEquals("Offsets should be in increasing order.", offsets.sorted, offsets);
    }

    /**
     * Test building an offset map off the log
     */
    @Test
    public void testBuildOffsetMap() {
        val map = new FakeOffsetMap(1000);
        val log = makeLog();
        val cleaner = makeCleaner(Integer.MAX_VALUE);
        val start = 0;
        val end = 500;
        val offsets = writeToLog(log, (start until end) zip(start until end));

    public void checkRange(FakeOffsetMap map, Integer start, Integer end) {
        val endOffset = cleaner.buildOffsetMap(log, start, end, map) + 1;
        Assert.assertEquals("Last offset should be the end offset.", end, endOffset);
        Assert.assertEquals("Should have the expected number of messages in the map.", end - start, map.size);
        for (i< -start until end)
        Assert.assertEquals("Should find all the keys", i.toLong, map.get(key(i)));
        Assert.assertEquals("Should not find a value too small", -1L, map.get(key(start - 1)));
        Assert.assertEquals("Should not find a value too large", -1L, map.get(key(end)));
    }

    val segments = log.logSegments.toSeq;

    checkRange(map, 0,segments(1).baseOffset.toInt);

    checkRange(map, segments(1).baseOffset.toInt,

    segments(3).baseOffset.toInt);

    checkRange(map, segments(3).baseOffset.toInt,log.logEndOffset.toInt);
}

    public Log makeLog(File dir, LogConfig config) throws IOException {
        return new Log(dir, config, 0L, time.scheduler, time);
    }


    public void noOpCheckDone(TopicAndPartition topicAndPartition) { /* do nothing */ }

    public  makeCleaner(Integer capacity, ActionWithP<TopicAndPartition> checkDone){
        return new Cleaner(id=0,
        offsetMap=new FakeOffsetMap(capacity),
        ioBufferSize=64*1024,
        maxIoBufferSize=64*1024,
        dupBufferLoadFactor=0.75,
        throttler=throttler,
        time=time,
        checkDone=checkDone);

public void writeToLog(Log log,Iterable seq<(Int,Int)>):Iterable<Long> ={
        for((key,value)<-seq)
        yield log.append(message(key,value)).firstOffset;
        }

public void key Integer id)=ByteBuffer.wrap(id.toString.getBytes);

public void message Integer key,Integer value)=
        new ByteBufferMessageSet(new Message(key=key.toString.getBytes,bytes=value.toString.getBytes));

public void deleteMessage Integer key)=
        new ByteBufferMessageSet(new Message(key=key.toString.getBytes,bytes=null));

        }

class FakeOffsetMapextends implements OffsetMap {
    public Integer slots;

    public Map<String, Long> map = new HashMap<>();

    public FakeOffsetMapextends(Integer slots) {
        this.slots = slots;
    }

    private String keyFor(ByteBuffer key) throws UnsupportedEncodingException {
        return new String(Utils.readBytes(key.duplicate()), "UTF-8");
    }

    @Override
    public Integer slots() {
        return slots;
    }

    public void put(ByteBuffer key, Long offset) throws UnsupportedEncodingException {
        map.put(keyFor(key), offset);
    }


    public Long get(ByteBuffer key) throws UnsupportedEncodingException {
        String k = keyFor(key);
        if (map.containsKey(k))
            return map.get(k);
        else
            return -1L;
    }

    public void clear() {
        map.clear();
    }

    public Integer size() {
        return map.size();
    }
}
