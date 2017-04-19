package kafka.log;

import com.google.common.collect.Lists;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.Utils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

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
     */
    @Test
   public void testGetNonExistentLog() {
        Optional<Log> log = logManager.getLog(new TopicAndPartition(name, 0));
       Assert.assertEquals("No log should be found.", Optional.empty(), log);
        File logFile = new File(logDir, name + "-0");
        Assert.assertTrue(!logFile.exists());
    }
//
//    /**
//     * Test time-based log cleanup. First append messages, then set the time into the future and run cleanup.
//     */
//    @Test
//   public void testCleanupExpiredSegments() {
//        val log = logManager.createLog(TopicAndPartition(name, 0), logConfig);
//        var offset = 0L;
//        for(int i = 0; i < 200; i++) {
//            var set = TestUtils.singleMessageSet("test".getBytes());
//            val info = log.append(set);
//            offset = info.lastOffset;
//        }
//        assertTrue("There should be more than one segment now.", log.numberOfSegments > 1);
//
//        log.logSegments.foreach(_.log.file.setLastModified(time.milliseconds))
//
//        time.sleep(maxLogAgeMs + 1);
//       Assert.assertEquals("Now there should only be only one segment in the index.", 1, log.numberOfSegments);
//        time.sleep(log.config.fileDeleteDelayMs + 1);
//       Assert.assertEquals("Files should have been deleted", log.numberOfSegments * 2, log.dir.list.length);
//       Assert.assertEquals("Should get empty fetch off new log.", 0, log.read(offset+1, 1024).messageSet.sizeInBytes);
//
//        try {
//            log.read(0, 1024);
//            fail("Should get exception from fetching earlier.");
//        } catch {
//            case OffsetOutOfRangeException e => "This is good.";
//        }
//        // log should still be appendable;
//        log.append(TestUtils.singleMessageSet("test".getBytes()));
//    }
//
//    /**
//     * Test size-based cleanup. Append messages, then run cleanup and check that segments are deleted.
//     */
//    @Test
//   public void testCleanupSegmentsToMaintainSize() {
//        val setSize = TestUtils.singleMessageSet("test".getBytes()).sizeInBytes;
//        logManager.shutdown();
//
//        val config = logConfig.copy(segmentSize = 10 * setSize, retentionSize = 5L * 10L * setSize + 10L);
//        logManager = createLogManager();
//        logManager.startup;
//
//        // create a log;
//        val log = logManager.createLog(TopicAndPartition(name, 0), config);
//        var offset = 0L;
//
//        // add a bunch of messages that should be larger than the retentionSize;
//        val numMessages = 200;
//        for(i <- 0 until numMessages) {
//            val set = TestUtils.singleMessageSet("test".getBytes());
//            val info = log.append(set);
//            offset = info.firstOffset;
//        }
//
//       Assert.assertEquals("Check we have the expected number of segments.", numMessages * setSize / config.segmentSize, log.numberOfSegments);
//
//        // this cleanup shouldn't find any expired segments but should delete some to reduce size;
//        time.sleep(logManager.InitialTaskDelayMs);
//       Assert.assertEquals("Now there should be exactly 6 segments", 6, log.numberOfSegments);
//        time.sleep(log.config.fileDeleteDelayMs + 1);
//       Assert.assertEquals("Files should have been deleted", log.numberOfSegments * 2, log.dir.list.length);
//       Assert.assertEquals("Should get empty fetch off new log.", 0, log.read(offset + 1, 1024).messageSet.sizeInBytes);
//        try {
//            log.read(0, 1024);
//            fail("Should get exception from fetching earlier.");
//        } catch {
//            case OffsetOutOfRangeException e => "This is good.";
//        }
//        // log should still be appendable;
//        log.append(TestUtils.singleMessageSet("test".getBytes()));
//    }
//
//    /**
//     * Test that flush is invoked by the background scheduler thread.
//     */
//    @Test
//   public void testTimeBasedFlush() {
//        logManager.shutdown();
//        val config = logConfig.copy(flushMs = 1000);
//        logManager = createLogManager();
//        logManager.startup;
//        val log = logManager.createLog(TopicAndPartition(name, 0), config);
//        val lastFlush = log.lastFlushTime;
//        for(int i = 0; i < 200; i++) {
//            var set = TestUtils.singleMessageSet("test".getBytes());
//            log.append(set);
//        }
//        time.sleep(logManager.InitialTaskDelayMs);
//        assertTrue("Time based flush should have been triggered triggered", lastFlush != log.lastFlushTime);
//    }
//
//    /**
//     * Test that new logs that are created are assigned to the least loaded log directory
//     */
//    @Test
//   public void testLeastLoadedAssignment() {
//        // create a log manager with multiple data directories;
//        val dirs = Array(TestUtils.tempDir(),
//                TestUtils.tempDir(),
//                TestUtils.tempDir());
//        logManager.shutdown();
//        logManager = createLogManager();
//
//        // verify that logs are always assigned to the least loaded partition;
//        for(int partition = 0; partition < 20; partition++) {
//            logManager.createLog(TopicAndPartition("test", partition), logConfig);
//           Assert.assertEquals("We should have created the right number of logs", partition + 1, logManager.allLogs.size);
//            val counts = logManager.allLogs.groupBy(_.dir.getParent).values.map(_.size);
//            assertTrue("Load should balance evenly", counts.max <= counts.min + 1);
//        }
//    }
//
//    /**
//     * Test that it is not possible to open two log managers using the same data directory
//     */
//    @Test
//   public void testTwoLogManagersUsingSameDirFails() {
//        try {
//            createLogManager();
//            fail("Should not be able to create a second log manager instance with the same data directory");
//        } catch {
//            case KafkaException e => // this is good;
//        }
//    }
//
//    /**
//     * Test that recovery points are correctly written out to disk
//     */
//    @Test
//   public void testCheckpointRecoveryPoints() {
//        verifyCheckpointRecovery(Seq(TopicAndPartition("test-a", 1), TopicAndPartition("test-b", 1)), logManager)
//    }
//
//    /**
//     * Test that recovery points directory checking works with trailing slash
//     */
//    @Test
//   public void testRecoveryDirectoryMappingWithTrailingSlash() {
//        logManager.shutdown();
//        logDir = TestUtils.tempDir();
//        logManager = TestUtils.createLogManager(
//                logDirs = Array(new File(logDir.getAbsolutePath + File.separator)));
//        logManager.startup;
//        verifyCheckpointRecovery(Seq(TopicAndPartition("test-a", 1)), logManager)
//    }
//
//    /**
//     * Test that recovery points directory checking works with relative directory
//     */
//    @Test
//   public void testRecoveryDirectoryMappingWithRelativeDirectory() {
//        logManager.shutdown();
//        logDir = new File("data" + File.separator + logDir.getName);
//        logDir.mkdirs();
//        logDir.deleteOnExit();
//        logManager = createLogManager();
//        logManager.startup;
//        verifyCheckpointRecovery(Seq(TopicAndPartition("test-a", 1)), logManager)
//    }
//
//
//    privatepublic void verifyCheckpointRecovery(Seq topicAndPartitions<TopicAndPartition>,
//                                         LogManager logManager) {
//        val logs = topicAndPartitions.map(this.logManager.createLog(_, logConfig));
//        logs.foreach(log => {
//        for(int i = 0; i < 50; i++)
//        log.append(TestUtils.singleMessageSet("test".getBytes()));
//
//        log.flush();
//    });
//
//        logManager.checkpointRecoveryPointOffsets();
//        val checkpoints = new OffsetCheckpoint(new File(logDir, logManager.RecoveryPointCheckpointFile)).read();
//
//        topicAndPartitions.zip(logs).foreach {
//            case(tp, log) => {
//               Assert.assertEquals("Recovery point should equal checkpoint", checkpoints(tp), log.recoveryPoint);
//            }
//        }
//    }
//
//
    private LogManager createLogManager() throws Exception {
        return TestUtils.createLogManager(Lists.newArrayList(this.logDir),
                logConfig,
                cleanerConfig,
                time);
    }

    private LogManager createLogManager(List<File> logDirs) throws Exception {
        return TestUtils.createLogManager(logDirs,
                logConfig,
                cleanerConfig,
                time);
    }
}
