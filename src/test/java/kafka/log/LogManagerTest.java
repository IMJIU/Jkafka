package kafka.log;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import kafka.common.KafkaException;
import kafka.common.OffsetOutOfRangeException;
import kafka.message.ByteBufferMessageSet;
import kafka.server.BrokerState;
import kafka.utils.MockTime;
import kafka.utils.Sc;
import kafka.utils.TestUtils;
import kafka.utils.Utils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author
 * @create 2017-04-19 37 9
 **/
public class LogManagerTest {

    MockTime time = new MockTime();
    Long maxRollInterval = 100L;
    Long maxLogAgeMs = 10 * 60 * 60 * 1000L;
    LogConfig logConfig;
    File logDir = null;
    LogManager logManager = null;
    String name = "kafka";
    Long veryLargeLogFlushInterval = 10000000L;
    CleanerConfig cleanerConfig = new CleanerConfig(false);

    @Before
    public void setUp() throws Exception {
        logConfig = new LogConfig();
        logConfig.segmentSize = 1024;
        logConfig.maxIndexSize = 4096;
        logConfig.retentionMs = maxLogAgeMs;
        logDir = TestUtils.tempDir();
        logManager = createLogManager();
        logManager.startup();
        logDir = logManager.logDirs.get(0);
    }

    @After
    public void tearDown() throws Throwable {
        if (logManager != null)
            logManager.shutdown();
        Utils.rm(logDir);
        logManager.logDirs.stream().forEach(f -> Utils.rm(f));
    }

    /**
     * Test that getOrCreateLog on a non-existent log creates a new log and that we can append to the new log.
     * 创建topic日志
     */
    @Test
    public void testCreateLog() throws IOException {
        Log log = logManager.createLog(new TopicAndPartition(name, 0), logConfig);
        File logFile = new File(logDir, name + "-0");
        Assert.assertTrue(logFile.exists());
        log.append(TestUtils.singleMessageSet("test".getBytes()));
    }


    /**
     * Test that get on a non-existent returns None and no log is created.
     * 获取不存在的log 返回optional empty()
     */
    @Test
    public void testGetNonExistentLog() {
        Optional<Log> log = logManager.getLog(new TopicAndPartition(name, 0));
        Assert.assertEquals("No log should be found.", Optional.empty(), log);
        File logFile = new File(logDir, name + "-0");
        Assert.assertTrue(!logFile.exists());
    }

    /**
     * Test time-based log cleanup. First append messages, then set the time into the future and run cleanup.
     */
    @Test
    public void testCleanupExpiredSegments() throws IOException {
        Log log = logManager.createLog(new TopicAndPartition(name, 0), logConfig);
        Long offset = 0L;
        for (int i = 0; i < 200; i++) {
            ByteBufferMessageSet set = TestUtils.singleMessageSet("test".getBytes());
            LogAppendInfo info = log.append(set);
            offset = info.lastOffset;
        }
        Assert.assertTrue("There should be more than one segment now.", log.numberOfSegments() > 1);

        log.logSegments().forEach(s -> s.log.file.setLastModified(time.milliseconds()));

        time.sleep(maxLogAgeMs + 1);//sleep过日志最大年龄
        Assert.assertEquals("Now there should only be only one segment in the index.", new Integer(1), log.numberOfSegments());
        time.sleep(log.config.fileDeleteDelayMs + 1);//sleep过删除延迟时间
        Assert.assertEquals("Files should have been deleted", log.numberOfSegments() * 2, log.dir.list().length);
        Assert.assertEquals("Should get empty fetch off new log.", new Integer(0), log.read(offset + 1, 1024).messageSet.sizeInBytes());

        try {
            log.read(0L, 1024);
            Assert.fail("Should get exception from fetching earlier.");
        } catch (OffsetOutOfRangeException e) {
            System.out.println("This is good.");
        }
        // log should still be appendable;
        log.append(TestUtils.singleMessageSet("test".getBytes()));
    }


    /**
     * Test size-based cleanup. Append messages, then run cleanup and check that segments are deleted.
     */
    @Test
    public void testCleanupSegmentsToMaintainSize() throws Throwable {
        Integer setSize = TestUtils.singleMessageSet("test".getBytes()).sizeInBytes();
        logManager.shutdown();

        LogConfig config = logConfig.copy(10 * setSize);
        config.retentionSize = 5L * 10L * setSize + 10L;
        logManager = createLogManager();
        logManager.startup();

        // create a log;
        Log log = logManager.createLog(new TopicAndPartition(name, 0), config);
        Long offset = 0L;

        // add a bunch of messages that should be larger than the retentionSize;
        Integer numMessages = 200;
        for (int i = 0; i < numMessages; i++) {
            ByteBufferMessageSet set = TestUtils.singleMessageSet("test".getBytes());
            LogAppendInfo info = log.append(set);
            offset = info.firstOffset;
        }

        Assert.assertEquals("Check we have the expected number of segments.", new Integer(numMessages * setSize / config.segmentSize), log.numberOfSegments());

        // this cleanup shouldn't find any expired segments but should delete some to reduce size;
        time.sleep(logManager.InitialTaskDelayMs);
        Assert.assertEquals("Now there should be exactly 6 segments", new Integer(6), log.numberOfSegments());
        time.sleep(log.config.fileDeleteDelayMs + 1);
        Assert.assertEquals("Files should have been deleted", log.numberOfSegments() * 2, log.dir.list().length);
        Assert.assertEquals("Should get empty fetch off new log.", new Integer(0), log.read(offset + 1, 1024).messageSet.sizeInBytes());

        try {
            log.read(0L, 1024);
            Assert.fail("Should get exception from fetching earlier.");
        } catch (OffsetOutOfRangeException e) {
            System.out.println("This is good.");
        }
        // log should still be appendable;
        log.append(TestUtils.singleMessageSet("test".getBytes()));
    }

    /**
     * Test that flush is invoked by the background scheduler thread.
     */
    @Test
    public void testTimeBasedFlush() throws Throwable {
        logManager.shutdown();
        LogConfig config = logConfig.clone();
        config.flushMs = 1000L;
        logManager = createLogManager();
        logManager.startup();
        Log log = logManager.createLog(new TopicAndPartition(name, 0), config);
        Long lastFlush = log.lastFlushTime();
        for (int i = 0; i < 200; i++) {
            ByteBufferMessageSet set = TestUtils.singleMessageSet("test".getBytes());
            log.append(set);
        }
        time.sleep(logManager.InitialTaskDelayMs);
        Assert.assertTrue("Time based flush should have been triggered triggered", lastFlush != log.lastFlushTime());
    }

    /**
     * Test that new logs that are created are assigned to the least loaded log directory
     */
    @Test
    public void testLeastLoadedAssignment() throws Throwable {
        // create a log manager with multiple data directories;
        List<File> dirs = Lists.newArrayList(TestUtils.tempDir(),
                TestUtils.tempDir(),
                TestUtils.tempDir());
        logManager.shutdown();
        logManager = createLogManager();

        // verify that logs are always assigned to the least loaded partition;
        for (int partition = 0; partition < 20; partition++) {
            logManager.createLog(new TopicAndPartition("test", partition), logConfig);
            Assert.assertEquals("We should have created the right number of logs", partition + 1, Utils.size(logManager.allLogs()));
            IntStream counts = Utils.groupBy(logManager.allLogs(), log -> log.dir.getParent()).values().stream().mapToInt(log -> log.size());
            Assert.assertTrue("Load should balance evenly", counts.max().getAsInt() <= counts.min().getAsInt() + 1);
        }
    }


    /**
     * Test that it is not possible to open two log managers using the same data directory
     */
    @Test
    public void testTwoLogManagersUsingSameDirFails() {
        try {
            createLogManager();
            Assert.fail("Should not be able to create a second log manager instance with the same data directory");
        } catch (KafkaException e) {
            System.out.println(" this is good;");
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * Test that recovery points are correctly written out to disk
     */
    @Test
    public void testCheckpointRecoveryPoints() throws IOException {
        verifyCheckpointRecovery(Lists.newArrayList(new TopicAndPartition("test-a", 1), new TopicAndPartition("test-b", 1)), logManager);
    }


    /**
     * Test that recovery points directory checking works with trailing slash
     */
    @Test
    public void testRecoveryDirectoryMappingWithTrailingSlash() throws Exception {
        logManager.shutdown();
        logDir = TestUtils.tempDir();
        logManager = createLogManager(Lists.newArrayList(new File(logDir.getAbsolutePath() + File.separator)));
        logManager.startup();
        verifyCheckpointRecovery(Lists.newArrayList(new TopicAndPartition("test-a", 1)), logManager);
    }

    /**
     * Test that recovery points directory checking works with relative directory
     */
    @Test
    public void testRecoveryDirectoryMappingWithRelativeDirectory() throws Exception {
        logManager.shutdown();
        logDir = new File("data" + File.separator + logDir.getName());
        logDir.mkdirs();
        logDir.deleteOnExit();
        logManager = createLogManager();
        logManager.startup();
        verifyCheckpointRecovery(Lists.newArrayList(new TopicAndPartition("test-a", 1)), logManager);
    }

    private void verifyCheckpointRecovery(List<TopicAndPartition> topicAndPartitions,
                                          LogManager logManager) throws IOException {
        List<Log> logs = Sc.map(topicAndPartitions, t -> this.logManager.createLog(t, logConfig));
        logs.forEach(log -> {
            for (int i = 0; i < 50; i++)
                log.append(TestUtils.singleMessageSet("test".getBytes()));
            log.flush();
        });

        logManager.checkpointRecoveryPointOffsets.invoke();
        Map<TopicAndPartition, Long> checkpoints = new OffsetCheckpoint(new File(logDir, logManager.RecoveryPointCheckpointFile)).read();
        for (int i = 0; i < topicAndPartitions.size(); i++) {
            TopicAndPartition tp = topicAndPartitions.get(i);
            Log log = logs.get(i);
            Assert.assertEquals("Recovery point should equal checkpoint", checkpoints.get(tp), log.recoveryPoint);
        }
    }


    private LogManager createLogManager() throws IOException {
        return new LogManager(
                Lists.newArrayList(this.logDir),
                Maps.newHashMap(),
                logConfig,
                cleanerConfig,
                4,
                1000L,
                10000L,
                1000L,
                time.scheduler,
                new BrokerState(),
                time
        );
    }

    private LogManager createLogManager(List<File> logDirs) throws IOException {
        return new LogManager(
                logDirs,
                Maps.newHashMap(),
                logConfig,
                cleanerConfig,
                4,
                1000L,
                10000L,
                1000L,
                time.scheduler,
                new BrokerState(),
                time
        );
    }
}
