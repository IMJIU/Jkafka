package kafka.log;/**
 * Created by zhoulf on 2017/4/11.
 */

import kafka.metrics.KafkaMetricsGroup;
import kafka.utils.Pool;
import kafka.utils.Throttler;
import kafka.utils.Time;

import java.io.File;

/**
 * The cleaner is responsible for removing obsolete records from logs which have the dedupe retention strategy.
 * A message with key K and offset O is obsolete if there exists a message with key K and offset O' such that O < O'.
 *
 * Each log can be thought of being split into two sections of a segments "clean" section which has previously been cleaned followed by a
 * "dirty" section that has not yet been cleaned. The active log segment is always excluded from cleaning.
 *
 * The cleaning is carried out by a pool of background threads. Each thread chooses the dirtiest log that has the "dedupe" retention policy
 * and cleans that. The dirtiness of the log is guessed by taking the ratio of bytes in the dirty section of the log to the total bytes in the log.
 *
 * To clean a log the cleaner first builds a mapping of key=>last_offset for the dirty section of the log. See kafka.log.OffsetMap for details of
 * the implementation of the mapping.
 *
 * Once the key=>offset map is built, the log is cleaned by recopying each log segment but omitting any key that appears in the offset map with a
 * higher offset than what is found in the segment (i.e. messages with a key that appears in the dirty section of the log).
 *
 * To avoid segments shrinking to very small sizes with repeated cleanings we implement a rule by which if we will merge successive segments when
 * doing a cleaning if their log and index size are less than the maximum log and index size prior to the clean beginning.
 *
 * Cleaned segments are swapped into the log as they become available.
 *
 * One nuance that the cleaner must handle is log truncation. If a log is truncated while it is being cleaned the cleaning of that log is aborted.
 *
 * Messages with null payload are treated as deletes for the purpose of log compaction. This means that they receive special treatment by the cleaner.
 * The cleaner will only retain delete records for a period of time to avoid accumulating space indefinitely. This period of time is configurable on a per-topic
 * basis and is measured from the time the segment enters the clean portion of the log (at which point any prior message with that key has been removed).
 * Delete markers in the clean section of the log that are older than this time will not be retained when log segments are being recopied as part of cleaning.
 */
public class LogCleaner extends KafkaMetricsGroup{
    public CleanerConfig config;
    public File[] logDirs;
    public Pool<TopicAndPartition, Log> logs;
    public Time time;
    /**
     * @param config Configuration parameters for the cleaner
     * @param logDirs The directories where offset checkpoints reside
     * @param logs The pool of logs
     * @param time A way to control the passage of time
     */
    public LogCleaner(CleanerConfig config, File[] logDirs, Pool<TopicAndPartition, Log> logs, Time time) {
        this.config = config;
        this.logDirs = logDirs;
        this.logs = logs;
        this.time = time;
    }


  /* for managing the state of partitions being cleaned. package-private to allow access in tests */
    LogCleanerManager cleanerManager = new LogCleanerManager(logDirs, logs);

  /* a throttle used to limit the I/O of all the cleaner threads to a user-specified maximum rate */
        private Throttler throttler = new Throttler(config.maxIoBytesPerSecond,
                300,
                true,
                "cleaner-io",
                "bytes",
                time);

  /* the threads */
        private val cleaners = (0 until config.numThreads).map(new CleanerThread(_));

  /* a metric to track the maximum utilization of any thread's buffer in the last cleaning */
        newGauge("max-buffer-utilization-percent",
                new Gauge<Integer> {
           public void Integer value = cleaners.map(_.lastStats).map(100 * _.bufferUtilization).max.toInt;
        });
  /* a metric to track the recopy rate of each thread's last cleaning */
        newGauge("cleaner-recopy-percent",
                new Gauge<Integer> {
           public void Integer value = {
                    val stats = cleaners.map(_.lastStats);
                    val recopyRate = stats.map(_.bytesWritten).sum.toDouble / math.max(stats.map(_.bytesRead).sum, 1);
                    (100 * recopyRate).toInt;
            }
        });
  /* a metric to track the maximum cleaning time for the last cleaning from each thread */
        newGauge("max-clean-time-secs",
                new Gauge<Integer> {
           public void Integer value = cleaners.map(_.lastStats).map(_.elapsedSecs).max.toInt;
        });

        /**
         * Start the background cleaning
         */
       public void startup() {
            info("Starting the log cleaner");
            cleaners.foreach(_.start())
        }

        /**
         * Stop the background cleaning
         */
       public void shutdown() {
            info("Shutting down the log cleaner.");
            cleaners.foreach(_.shutdown())
        }

        /**
         *  Abort the cleaning of a particular partition, if it's in progress. This call blocks until the cleaning of
         *  the partition is aborted.
         */
       public void abortCleaning(TopicAndPartition topicAndPartition) {
            cleanerManager.abortCleaning(topicAndPartition);
        }

        /**
         * Update checkpoint file, removing topics and partitions that no longer exist
         */
       public void updateCheckpoints(File dataDir) {
            cleanerManager.updateCheckpoints(dataDir, update=None);
        }

        /**
         *  Abort the cleaning of a particular partition if it's in progress, and pause any future cleaning of this partition.
         *  This call blocks until the cleaning of the partition is aborted and paused.
         */
       public void abortAndPauseCleaning(TopicAndPartition topicAndPartition) {
            cleanerManager.abortAndPauseCleaning(topicAndPartition);
        }

        /**
         *  Resume the cleaning of a paused partition. This call blocks until the cleaning of a partition is resumed.
         */
       public void resumeCleaning(TopicAndPartition topicAndPartition) {
            cleanerManager.resumeCleaning(topicAndPartition);
        }

        /**
         * TODO:
         * For testing, a way to know when work has completed. This method blocks until the
         * cleaner has processed up to the given offset on the specified topic/partition
         */
       public Unit  void awaitCleaned(String topic, Integer part, Long offset, Long timeout = 30000L) {
        while(!cleanerManager.allCleanerCheckpoints.contains(TopicAndPartition(topic, part)));
            Thread.sleep(10);
  }

        /**
         * The cleaner threads do the actual log cleaning. Each thread processes does its cleaning repeatedly by
         * choosing the dirtiest log, cleaning it, and then swapping in the cleaned segments.
         */
        private class CleanerThread(Integer threadId);
    extends ShutdownableThread(name = "kafka-log-cleaner-thread-" + threadId, isInterruptible = false) {

            override val loggerName = classOf<LogCleaner>.getName;

            if(config.dedupeBufferSize / config.numThreads > Integer.MAX_VALUE)
                warn("Cannot use more than 2G of cleaner buffer space per cleaner thread, ignoring excess buffer space...");

            val cleaner = new Cleaner(id = threadId,
                    offsetMap = new SkimpyOffsetMap(memory = math.min(config.dedupeBufferSize / config.numThreads, Integer.MAX_VALUE).toInt,
                            hashAlgorithm = config.hashAlgorithm),
                    ioBufferSize = config.ioBufferSize / config.numThreads / 2,
                    maxIoBufferSize = config.maxMessageSize,
                    dupBufferLoadFactor = config.dedupeBufferLoadFactor,
                    throttler = throttler,
                    time = time,
                    checkDone = checkDone);

    @volatile var CleanerStats lastStats = new CleanerStats()
            private val backOffWaitLatch = new CountDownLatch(1);

            privatepublic void checkDone(TopicAndPartition topicAndPartition) {
                if (!isRunning.get())
                    throw new ThreadShutdownException;
                cleanerManager.checkCleaningAborted(topicAndPartition);
            }

            /**
             * The main loop for the cleaner thread
             */
            overridepublic void doWork() {
                cleanOrSleep();
            }


            overridepublic void shutdown() = {
                    initiateShutdown();
                    backOffWaitLatch.countDown();
                    awaitShutdown();
            }

            /**
             * Clean a log if there is a dirty log available, otherwise sleep for a bit
             */
        privatepublic void cleanOrSleep() {
            cleanerManager.grabFilthiestLog() match {
                case None =>
                    // there are no cleanable logs, sleep a while;
                    backOffWaitLatch.await(config.backOffMs, TimeUnit.MILLISECONDS);
                case Some(cleanable) =>
                    // there's a log, clean it;
                    var endOffset = cleanable.firstDirtyOffset;
                    try {
                        endOffset = cleaner.clean(cleanable);
                        recordStats(cleaner.id, cleanable.log.name, cleanable.firstDirtyOffset, endOffset, cleaner.stats);
                    } catch {
                    case LogCleaningAbortedException pe => // task can be aborted, let it go.;
                } finally {
                    cleanerManager.doneCleaning(cleanable.topicPartition, cleanable.log.dir.getParentFile, endOffset);
                }
            }
        }

        /**
         * Log out statistics on a single run of the cleaner.
         */
       public void recordStats(Integer id, String name, Long from, Long to, CleanerStats stats) {
            this.lastStats = stats;
            cleaner.statsUnderlying.swap;
           public void mb(Double bytes) = bytes / (1024*1024);
            val message =
                    String.format("%n\tLog cleaner thread %d cleaned log %s (dirty section = <%d, %d>)%n",id, name, from, to) +;
                            String.format("\t%,.1f MB of log processed in %,.1f seconds (%,.1f MB/sec).%n",mb(stats.bytesRead),
                                    stats.elapsedSecs,
                                    mb(stats.bytesRead/stats.elapsedSecs)) +;
                            String.format("\tIndexed %,.1f MB in %.1f seconds (%,.1f Mb/sec, %.1f%% of total time)%n",mb(stats.mapBytesRead),
                                    stats.elapsedIndexSecs,
                                    mb(stats.mapBytesRead)/stats.elapsedIndexSecs,
                                    100 * stats.elapsedIndexSecs.toDouble/stats.elapsedSecs) +;
                            String.format("\tBuffer utilization: %.1f%%%n",100 * stats.bufferUtilization) +;
                            String.format("\tCleaned %,.1f MB in %.1f seconds (%,.1f Mb/sec, %.1f%% of total time)%n",mb(stats.bytesRead),
                                    stats.elapsedSecs - stats.elapsedIndexSecs,
                                    mb(stats.bytesRead)/(stats.elapsedSecs - stats.elapsedIndexSecs), 100 * (stats.elapsedSecs - stats.elapsedIndexSecs).toDouble/stats.elapsedSecs) +;
                            String.format("\tStart size: %,.1f MB (%,d messages)%n",mb(stats.bytesRead), stats.messagesRead) +;
                            String.format("\tEnd size: %,.1f MB (%,d messages)%n",mb(stats.bytesWritten), stats.messagesWritten) +;
                            String.format("\t%.1f%% size reduction (%.1f%% fewer messages)%n",100.0 * (1.0 - stats.bytesWritten.toDouble/stats.bytesRead),
                                    100.0 * (1.0 - stats.messagesWritten.toDouble/stats.messagesRead));
            info(message);
        }

  }
    }
}
