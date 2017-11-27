package kafka.log;/**
 * Created by zhoulf on 2017/4/11.
 */

import com.yammer.metrics.core.Gauge;
import kafka.common.LogCleaningAbortedException;
import kafka.common.ThreadShutdownException;
import kafka.func.ActionP;
import kafka.metrics.KafkaMetricsGroup;
import kafka.utils.Pool;
import kafka.utils.ShutdownableThread;
import kafka.utils.Throttler;
import kafka.utils.Time;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The cleaner is responsible for removing obsolete records from logs which have the dedupe retention strategy.
 * A message with key K and offset O is obsolete if there exists a message with key K and offset O' such that O < O'.
 * <p>
 * Each log can be thought of being split into two sections of a segments "clean" section which has previously been cleaned followed by a
 * "dirty" section that has not yet been cleaned. The active log segment is always excluded from cleaning.
 * <p>
 * The cleaning is carried out by a pool of background threads. Each thread chooses the dirtiest log that has the "dedupe" retention policy
 * and cleans that. The dirtiness of the log is guessed by taking the ratio of bytes in the dirty section of the log to the total bytes in the log.
 * <p>
 * To clean a log the cleaner first builds a mapping of key=>last_offset for the dirty section of the log. See kafka.log.OffsetMap for details of
 * the implementation of the mapping.
 * <p>
 * Once the key=>offset map is built, the log is cleaned by recopying each log segment but omitting any key that appears in the offset map with a
 * higher offset than what is found in the segment (i.e. messages with a key that appears in the dirty section of the log).
 * <p>
 * To avoid segments shrinking to very small sizes with repeated cleanings we implement a rule by which if we will merge successive segments when
 * doing a cleaning if their log and index size are less than the maximum log and index size prior to the clean beginning.
 * <p>
 * Cleaned segments are swapped into the log as they become available.
 * <p>
 * One nuance that the cleaner must handle is log truncation. If a log is truncated while it is being cleaned the cleaning of that log is aborted.
 * <p>
 * Messages with null payload are treated as deletes for the purpose of log compaction. This means that they receive special treatment by the cleaner.
 * The cleaner will only retain delete records for a period of time to avoid accumulating space indefinitely. This period of time is configurable on a per-topic
 * basis and is measured from the time the segment enters the clean portion of the log (at which point any prior message with that key has been removed).
 * Delete markers in the clean section of the log that are older than this time will not be retained when log segments are being recopied as part of cleaning.
 */
public class LogCleaner extends KafkaMetricsGroup {
    public CleanerConfig config;
    public List<File> logDirs;
    public Pool<TopicAndPartition, Log> logs;
    public Time time;

    /* for managing the state of partitions being cleaned. package-private to allow access in tests */
    LogCleanerManager cleanerManager;

    /* a throttle used to limit the I/O of all the cleaner threads to a user-specified maximum rate */
    private Throttler throttler;

    /* the threads */
    private List<CleanerThread> cleaners;

    /**
     * @param config  Configuration parameters for the cleaner
     * @param logDirs The directories where offset checkpoints reside
     * @param logs    The pool of logs
     * @param time    A way to control the passage of time
     */
    public LogCleaner(CleanerConfig config, List<File> logDirs, Pool<TopicAndPartition, Log> logs, Time time) {
        this.config = config;
        this.logDirs = logDirs;
        this.logs = logs;
        this.time = time;
        init();
    }

    public void init() {
        cleaners = Stream.iterate(0, n -> n + 1).limit(config.numThreads).map(n -> new CleanerThread(n)).collect(Collectors.toList());
        cleanerManager = new LogCleanerManager(logDirs, logs);
        throttler = new Throttler(config.maxIoBytesPerSecond, 300L, true, "cleaner-io", "bytes", time);

        /* a metric to track the maximum utilization of any thread's buffer in the last cleaning */
        newGauge("max-buffer-utilization-percent", new Gauge<Object>() {
            @Override
            public Integer value() {
                return new Double(cleaners.stream().map(c -> c.lastStats).mapToDouble(s -> 100 * s.bufferUtilization).max().getAsDouble()).intValue();
            }
        });

    /* a metric to track the recopy rate of each thread's last cleaning */
        newGauge("cleaner-recopy-percent",
                new Gauge<Object>() {
                    @Override
                    public Integer value() {
                        List<CleanerStats> stats = cleaners.stream().map(c -> c.lastStats).collect(Collectors.toList());
                        Double recopyRate = 1D * stats.stream().mapToLong(s -> s.bytesWritten).sum() / Math.max(stats.stream().mapToLong(s -> s.bytesRead).sum(), 1);
                        return (int) (100 * recopyRate);
                    }
                });

    /* a metric to track the maximum cleaning time for the last cleaning from each thread */
        newGauge("max-clean-time-secs", new Gauge<Object>() {
            @Override
            public Integer value() {
                return new Double(cleaners.stream().map(c -> c.lastStats).mapToDouble(s -> s.elapsedSecs).max().getAsDouble()).intValue();
            }
        });
    }


    /**
     * Start the background cleaning
     */
    public void startup() {
        info("Starting the log cleaner");
        cleaners.forEach(c -> c.start());
    }

    /**
     * Stop the background cleaning
     */
    public void shutdown() {
        info("Shutting down the log cleaner.");
        cleaners.forEach(c -> {
            c.shutdown();
        });
    }

    /**
     * Abort the cleaning of a particular partition, if it's in progress. This call blocks until the cleaning of
     * the partition is aborted.
     */
    public void abortCleaning(TopicAndPartition topicAndPartition) {
        cleanerManager.abortCleaning(topicAndPartition);
    }

    /**
     * Update checkpoint file, removing topics and partitions that no longer exist
     */
    public void updateCheckpoints(File dataDir) {
        cleanerManager.updateCheckpoints(dataDir, Optional.empty());
    }

    /**
     * Abort the cleaning of a particular partition if it's in progress, and pause any future cleaning of this partition.
     * This call blocks until the cleaning of the partition is aborted and paused.
     */
    public void abortAndPauseCleaning(TopicAndPartition topicAndPartition) {
        cleanerManager.abortAndPauseCleaning(topicAndPartition);
    }

    /**
     * Resume the cleaning of a paused partition. This call blocks until the cleaning of a partition is resumed.
     */
    public void resumeCleaning(TopicAndPartition topicAndPartition) {
        cleanerManager.resumeCleaning(topicAndPartition);
    }

    /**
     * TODO:
     * For testing, a way to know when work has completed. This method blocks until the
     * cleaner has processed up to the given offset on the specified topic/partition
     */
    public void awaitCleaned(String topic, Integer part, Long offset, Long timeout) throws IOException, InterruptedException {
        if (timeout == null) {
            timeout = 30000L;
        }
        while (!cleanerManager.allCleanerCheckpoints().containsKey(new TopicAndPartition(topic, part)))
            Thread.sleep(10);
    }


    /**
     * The cleaner threads do the actual log cleaning. Each thread processes does its cleaning repeatedly by
     * choosing the dirtiest log, cleaning it, and then swapping in the cleaned segments.
     */
    private class CleanerThread extends ShutdownableThread {
        public Integer threadId;
        public Cleaner cleaner;

        public CleanerThread(String name, Boolean isInterruptible, Integer threadId) {
            super(name, isInterruptible);
            this.threadId = threadId;
        }

        public CleanerThread(Integer threadId) {
            super("kafka-log-cleaner-thread-" + threadId, false);
            this.threadId = threadId;
            logger.loggerName(LogCleaner.class.getName());
        }

        public void init() {
            if (config.dedupeBufferSize / config.numThreads > Integer.MAX_VALUE)
                warn("Cannot use more than 2G of cleaner buffer space per cleaner thread, ignoring excess buffer space...");
            SkimpyOffsetMap offsetMap = new SkimpyOffsetMap((int) Math.min(config.dedupeBufferSize / config.numThreads, Integer.MAX_VALUE), config.hashAlgorithm);
            cleaner = new Cleaner(threadId,
                    offsetMap,
                    config.ioBufferSize / config.numThreads / 2,
                    config.maxMessageSize,
                    config.dedupeBufferLoadFactor,
                    throttler,
                    time,
                    checkDone);
            lastStats = new CleanerStats(time);
        }


        public volatile CleanerStats lastStats;
        private CountDownLatch backOffWaitLatch = new CountDownLatch(1);
        private ActionP<TopicAndPartition> checkDone = (topicAndPartition) -> {
            if (!isRunning.get())
                throw new ThreadShutdownException();
            cleanerManager.checkCleaningAborted(topicAndPartition);
        };

        /**
         * The main loop for the cleaner thread
         */
        @Override
        public void doWork() {
            try {
                cleanOrSleep();
            } catch (InterruptedException e) {
                error(e.getMessage(), e);
            } catch (IOException e) {
                error(e.getMessage(), e);
            }
        }

        @Override
        public void shutdown() {
            initiateShutdown();
            backOffWaitLatch.countDown();
            awaitShutdown();
        }

        /**
         * Clean a log if there is a dirty log available, otherwise sleep for a bit
         */
        private void cleanOrSleep() throws InterruptedException, IOException {
            Optional<LogToClean> optional = cleanerManager.grabFilthiestLog();
            if (optional.isPresent()) {
                LogToClean cleanable = optional.get();
                // there's a log, clean it;
                Long endOffset = cleanable.firstDirtyOffset;
                try {
                    endOffset = cleaner.clean(cleanable);
                    recordStats(cleaner.id, cleanable.log.name, cleanable.firstDirtyOffset, endOffset, cleaner.stats);
                } catch (LogCleaningAbortedException e) {
                    //task can be aborted, let it go.;
                } finally {
                    cleanerManager.doneCleaning(cleanable.topicPartition, cleanable.log.dir.getParentFile(), endOffset);
                }
            } else {
                backOffWaitLatch.await(config.backOffMs, TimeUnit.MILLISECONDS);
            }
        }

        public Double mb(Double bytes) {
            return bytes / (1024 * 1024);
        }

        /**
         * Log out statistics on a single run of the cleaner.
         */
        public void recordStats(Integer id, String name, Long from, Long to, CleanerStats stats) {
            this.lastStats = stats;
            CleanerStats tmp = cleaner.statsUnderlying.v1;
            cleaner.statsUnderlying.v1 = cleaner.statsUnderlying.v2;
            cleaner.statsUnderlying.v2 = tmp;


            String message =
                    String.format("%n\tLog cleaner thread %d cleaned log %s (dirty section = <%d, %d>)%n", id, name, from, to) +
                            String.format("\t%,.1f MB of log processed in %,.1f seconds (%,.1f MB/sec).%n", mb(stats.bytesRead.doubleValue()),
                                    stats.elapsedSecs,
                                    mb(stats.bytesRead / stats.elapsedSecs)) +
                            String.format("\tIndexed %,.1f MB in %.1f seconds (%,.1f Mb/sec, %.1f%% of total time)%n", mb(stats.mapBytesRead.doubleValue()),
                                    stats.elapsedIndexSecs,
                                    mb(stats.mapBytesRead.doubleValue()) / stats.elapsedIndexSecs,
                                    100 * stats.elapsedIndexSecs.doubleValue() / stats.elapsedSecs) +
                            String.format("\tBuffer utilization: %.1f%%%n", 100 * stats.bufferUtilization) +
                            String.format("\tCleaned %,.1f MB in %.1f seconds (%,.1f Mb/sec, %.1f%% of total time)%n", mb(stats.bytesRead.doubleValue()),
                                    stats.elapsedSecs - stats.elapsedIndexSecs,
                                    mb(stats.bytesRead.doubleValue()) / (stats.elapsedSecs - stats.elapsedIndexSecs), 100 * (stats.elapsedSecs - stats.elapsedIndexSecs) / stats.elapsedSecs) +
                            String.format("\tStart size: %,.1f MB (%,d messages)%n", mb(stats.bytesRead.doubleValue()), stats.messagesRead) +
                            String.format("\tEnd size: %,.1f MB (%,d messages)%n", mb(stats.bytesWritten.doubleValue()), stats.messagesWritten) +
                            String.format("\t%.1f%% size reduction (%.1f%% fewer messages)%n", 100.0 * (1.0 - stats.bytesWritten.doubleValue() / stats.bytesRead),
                                    100.0 * (1.0 - stats.messagesWritten.doubleValue() / stats.messagesRead));

            info(message);
        }
    }
}

/**
 * Helper class for a log, its topic/partition, and the last clean position
 */
class LogToClean implements Comparable<LogToClean> {
    public TopicAndPartition topicPartition;
    public Log log;
    public Long firstDirtyOffset;

    public LogToClean(TopicAndPartition topicPartition, Log log, Long firstDirtyOffset) {
        this.topicPartition = topicPartition;
        this.log = log;
        this.firstDirtyOffset = firstDirtyOffset;
        init();
    }

    public Long cleanBytes;
    public Long dirtyBytes;
    Double cleanableRatio;

    public void init() {
        cleanBytes = log.logSegments(-1L, firstDirtyOffset - 1).stream().mapToLong(s -> s.size()).sum();
        dirtyBytes = log.logSegments(firstDirtyOffset, Math.max(firstDirtyOffset, log.activeSegment().baseOffset)).stream().mapToLong(s -> s.size()).sum();
        cleanableRatio = dirtyBytes / (double) totalBytes();
    }

    public Long totalBytes() {
        return cleanBytes + dirtyBytes;
    }

    public int compare(LogToClean o1, LogToClean o2) {
        return (int) Math.signum(o1.cleanableRatio - o2.cleanableRatio);
    }

    @Override
    public int compareTo(LogToClean o) {
        return (int) Math.signum(this.cleanableRatio - o.cleanableRatio);
    }
}
