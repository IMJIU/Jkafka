package kafka.log;/**
 * Created by zhoulf on 2017/4/10.
 */

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.UnmodifiableIterator;
import kafka.common.LogCleaningAbortedException;
import kafka.func.ActionWithP;
import kafka.func.Tuple;
import kafka.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.Throttler;
import kafka.utils.Utils;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author
 * @create 2017-04-10 43 17
 **/
public class CleanerTest {

    File dir = TestUtils.tempDir();
    LogConfig logConfig = new LogConfig();

    MockTime time = new MockTime();
    Throttler throttler = new Throttler(Double.MAX_VALUE, Long.MAX_VALUE, time);

    @Before
    public void setup() {
        logConfig.segmentSize = 1024;
        logConfig.maxIndexSize = 1024;
        logConfig.compact = true;
    }

    @After
    public void teardown() {
        Utils.rm(dir);
    }

    /**
     * Test simple log cleaning
     */
    @Test
    public void testCleanSegments() throws IOException {
        Cleaner cleaner = makeCleaner(Integer.MAX_VALUE, abortCheckDone);
        Log log = makeLog(dir, logConfig.copy(1024));

        // append messages to the log until we have four segments;
        while (log.numberOfSegments() < 4)
            log.append(message(log.logEndOffset().intValue(), log.logEndOffset().intValue()));
        List<Integer> keysFound = keysInLog(log);
        TestUtils.checkEquals(Stream.iterate(0, n -> n + 1).limit(log.logEndOffset()).iterator(), keysFound.iterator());

        // pretend we have the following keys;
        List<Integer> keys = Lists.newArrayList(1, 3, 5, 7, 9);
        FakeOffsetMap map = new FakeOffsetMap(Integer.MAX_VALUE);
        keys.forEach(k -> {
            try {
                map.put(key(k), Long.MAX_VALUE);
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        });

        // clean the log;
        cleaner.cleanSegments(log, Lists.newArrayList(log.logSegments()).subList(0, 3), map, 0L);
        List<Integer> list = keysInLog(log);
        TestUtils.checkEquals(list.stream().filter(l -> !keys.contains(l)).iterator(), keysInLog(log).iterator());
    }

    @Test
    public void testCleaningWithDeletes() throws IOException, InterruptedException {
        Cleaner cleaner = makeCleaner(Integer.MAX_VALUE);
        Log log = makeLog(logConfig.copy(1024));

        // append messages with the keys 0 through N;
        while (log.numberOfSegments() < 2) ;
        log.append(message(log.logEndOffset().intValue(), log.logEndOffset().intValue()));

        // delete all even keys between 0 and N;
        Long leo = log.logEndOffset();
        for (int key = 0; key < leo.intValue(); key += 2)
            log.append(deleteMessage(key));

        // append some new unique keys to pad out to a new active segment;
        while (log.numberOfSegments() < 4)
            log.append(message(log.logEndOffset().intValue(), log.logEndOffset().intValue()));

        cleaner.clean(new LogToClean(new TopicAndPartition("test", 0), log, 0L));
        List<Integer> keys = Lists.newArrayList(keysInLog(log));
        Assert.assertTrue("None of the keys we deleted should still exist.",
                Stream.iterate(0, n -> n + 2).limit(leo).allMatch(n -> !keys.contains(n)));
    }

    /* extract all the keys from a log */
    public List<Integer> keysInLog(Log log) {
        return log.logSegments().stream()
                .flatMap(s -> s.log.toMessageAndOffsetList().stream()
                                .filter(l -> !l.message.isNull())
                                .map(m -> Integer.parseInt(Utils.readString(m.message.key()))))
                .collect(Collectors.toList());
    }

    public ActionWithP<TopicAndPartition> noOpCheckDone = (topicAndPartition) -> {
        /* do nothing */
    };
    public ActionWithP<TopicAndPartition> abortCheckDone = (topicAndPartition) -> {
        throw new LogCleaningAbortedException();
    };

    /**
     * Test that abortion during cleaning throws a LogCleaningAbortedException
     */
    @Test
    public void testCleanSegmentsWithAbort() throws IOException {
        Cleaner cleaner = makeCleaner(Integer.MAX_VALUE, abortCheckDone);
        Log log = makeLog(logConfig.copy(1024));

        // append messages to the log until we have four segments;
        while (log.numberOfSegments() < 4) ;
        log.append(message(log.logEndOffset().intValue(), log.logEndOffset().intValue()));

        List<Integer> keys = keysInLog(log);
        FakeOffsetMap map = new FakeOffsetMap(Integer.MAX_VALUE);
        Lists.newArrayList(keys).forEach(k -> {
            try {
                map.put(key(k), Long.MAX_VALUE);
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        });
        thrown.expect(LogCleaningAbortedException.class);
        cleaner.cleanSegments(log, Lists.newArrayList(log.logSegments()).subList(0, 3), map, 0L);
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    /**
     * Validate the logic for grouping log segments together for cleaning
     */
    @Test
    public void testSegmentGrouping() throws IOException {
//        Cleaner cleaner = makeCleaner(Integer.MAX_VALUE);
//        LogConfig config = logConfig.copy(300);
//        config.indexInterval = 1;
//        Log log = makeLog(config);
//
//        // append some messages to the log;
//        int i = 0;
//        while (log.numberOfSegments() < 10) {
//            log.append(TestUtils.singleMessageSet("hello".getBytes()));
//            i += 1;
//        }
//
//        // grouping by very large values should result in a single group with all the segments in it;
//        List<List<LogSegment>> groups = cleaner.groupSegmentsBySize(log.logSegments(), Integer.MAX_VALUE, Integer.MAX_VALUE);
//        Assert.assertEquals(1, groups.size());
//        Assert.assertEquals(new Long(log.numberOfSegments()), new Long(groups.get(0).size()));
//        checkSegmentOrder(groups);
//
//        // grouping by very small values should result in all groups having one entry;
//        groups = cleaner.groupSegmentsBySize(log.logSegments(), 1, Integer.MAX_VALUE);
//        Assert.assertEquals(log.numberOfSegments(), new Integer(groups.size()));
//        Assert.assertTrue("All groups should be singletons.", groups.forall(_.size == 1));
//        checkSegmentOrder(groups);
//        groups = cleaner.groupSegmentsBySize(log.logSegments(), Integer.MAX_VALUE, 1);
//        Assert.assertEquals(log.numberOfSegments(), new Integer(groups.size()));
//        Assert.assertTrue("All groups should be singletons.", groups.forall(_.size == 1));
//        checkSegmentOrder(groups);
//
//        Integer groupSize = 3;
//
//        // check grouping by log size;
//        Integer logSize = log.logSegments().take(groupSize).map(_.size).sum.toInt + 1;
//        groups = cleaner.groupSegmentsBySize(log.logSegments(), logSize, Integer.MAX_VALUE);
//        checkSegmentOrder(groups);
//        Assert.assertTrue("All but the last group should be the target size.", groups.dropRight(1).forall(_.size == groupSize))
//
//        // check grouping by index size;
//        Integer indexSize = log.logSegments().take(groupSize).map(_.index.sizeInBytes()).sum + 1;
//        groups = cleaner.groupSegmentsBySize(log.logSegments(), Integer.MAX_VALUE, indexSize);
//        checkSegmentOrder(groups);
//        Assert.assertTrue("All but the last group should be the target size.", groups.dropRight(1).forall(_.size == groupSize))
    }

    private void checkSegmentOrder(List<List<LogSegment>> groups) {
        List<Long> offsets = groups.stream().flatMap(l->l.stream().map(s->s.baseOffset)).collect(Collectors.toList());
        List<Long>before = Lists.newArrayList();
        before.addAll(offsets);
        offsets.sort((o1, o2) -> o1>o2?1:0);
        //"Offsets should be in increasing order.",
        TestUtils.checkEquals(offsets.iterator(), before.iterator());
    }

    /**
     * Test building an offset map off the log
     */
    @Test
    public void testBuildOffsetMap() throws IOException, InterruptedException {
        FakeOffsetMap map = new FakeOffsetMap(1000);
        Log log = makeLog(dir, logConfig);
        Cleaner cleaner = makeCleaner(Integer.MAX_VALUE);
        Integer start = 0;
        Integer end = 500;
        List<Tuple<Integer, Integer>> list = Stream.iterate(start, n -> n + 1).limit(end).map(n -> Tuple.of(n, n)).collect(Collectors.toList());
        Iterable<Long> offsets = writeToLog(log, list);
        List<LogSegment> segments = Lists.newArrayList(log.logSegments());

        checkRange(cleaner, log, map, 0, segments.get(1).baseOffset.intValue());

        checkRange(cleaner, log, map, segments.get(1).baseOffset.intValue(), segments.get(3).baseOffset.intValue());

        checkRange(cleaner, log, map, segments.get(3).baseOffset.intValue(), log.logEndOffset().intValue());
    }

    public void checkRange(Cleaner cleaner, Log log, FakeOffsetMap map, Integer start, Integer end) throws IOException, InterruptedException {
        Long endOffset = cleaner.buildOffsetMap(log, start.longValue(), end.longValue(), map) + 1;
        Assert.assertEquals("Last offset should be the end offset.", end, endOffset);
        Assert.assertEquals("Should have the expected number of messages in the map.", new Integer(end - start), map.size());
        for (int i = start; i < end; i++)
            Assert.assertEquals("Should find all the keys", new Long(i), map.get(key(i)));
        Assert.assertEquals("Should not find a value too small", new Long(-1), map.get(key(start - 1)));
        Assert.assertEquals("Should not find a value too large", new Long(-1), map.get(key(end)));
    }


    public Log makeLog(LogConfig config) throws IOException {
        return new Log(dir, config, 0L, time.scheduler, time);
    }

    public Log makeLog(File dir, LogConfig config) throws IOException {
        return new Log(dir, config, 0L, time.scheduler, time);
    }

    public Cleaner makeCleaner(Integer capacity) {
        return makeCleaner(capacity, noOpCheckDone);
    }

    public Cleaner makeCleaner(Integer capacity, ActionWithP<TopicAndPartition> checkDone) {
        return new Cleaner(0,
                new FakeOffsetMap(capacity),
                64 * 1024,
                64 * 1024,
                0.75,
                throttler,
                time,
                checkDone);
    }

    public Iterable<Long> writeToLog(Log log, List<Tuple<Integer, Integer>> seq) {
        List<Long> result = Lists.newArrayList();
        for (Tuple<Integer, Integer> kv : seq)
            result.add(log.append(message(kv.v1, kv.v2)).firstOffset);
        return result;
    }

    public ByteBuffer key(Integer id) {
        return ByteBuffer.wrap(id.toString().getBytes());
    }

    public ByteBufferMessageSet message(Integer key, Integer value) {
        return new ByteBufferMessageSet(new Message(value.toString().getBytes(), key.toString().getBytes()));
    }


    public ByteBufferMessageSet deleteMessage(Integer key) {
        return new ByteBufferMessageSet(new Message(null, key.toString().getBytes()));
    }

    class FakeOffsetMap implements OffsetMap {
        public Integer slots;

        public Map<String, Long> map = new HashMap<>();

        public FakeOffsetMap(Integer slots) {
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
}
