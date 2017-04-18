package kafka.log;/**
 * Created by zhoulf on 2017/4/12.
 */

import com.google.common.collect.Maps;
import com.yammer.metrics.core.Gauge;
import kafka.common.LogCleaningAbortedException;
import kafka.func.Tuple;
import kafka.metrics.KafkaMetricsGroup;
import kafka.utils.Pool;
import kafka.utils.Utils;
//import org.elasticsearch.index.fielddata.IndexFieldDataCache;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * Manage the state of each partition being cleaned.
 * If a partition is to be cleaned, it enters the LogCleaningInProgress state.
 * While a partition is being cleaned, it can be requested to be aborted and paused. Then the partition first enters
 * the LogCleaningAborted state. Once the cleaning task is aborted, the partition enters the LogCleaningPaused state.
 * While a partition is in the LogCleaningPaused state, it won't be scheduled for cleaning again, until cleaning is
 * requested to be resumed.
 */
public class LogCleanerManager extends KafkaMetricsGroup {
    List<File> logDirs;
    Pool<TopicAndPartition, Log> logs;


    public LogCleanerManager(List<File> logDirs, Pool<TopicAndPartition, Log> logs) {
        this.logDirs = logDirs;
        this.logs = logs;
        loggerName(LogCleanerManager.class.getName());
    }

    public void init() {
        checkpoints = Maps.newHashMap();
        for (File dir : logDirs) {
            try {
                checkpoints.put(dir, new OffsetCheckpoint(new File(dir, offsetCheckpointFile)));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        newGauge("max-dirty-percent", new Gauge<Integer>() {
            @Override
            public Integer value() {
                return (int) (100 * dirtiestLogCleanableRatio);
            }
        });

    }

    // package-private for testing;
    String offsetCheckpointFile = "cleaner-offset-checkpoint";

    /* the offset checkpoints holding the last cleaned point for each log */
    private Map<File, OffsetCheckpoint> checkpoints;

    /* the set of logs currently being cleaned */
    private Map<TopicAndPartition, LogCleaningState> inProgress = Maps.newHashMap();

    /* a global lock used to control all access to the in-progress set and the offset checkpoints */
    private ReentrantLock lock = new ReentrantLock();

    /* for coordinating the pausing and the cleaning of a partition */
    private Condition pausedCleaningCond = lock.newCondition();

    /* a gauge for tracking the cleanable ratio of the dirtiest log */
    volatile private Double dirtiestLogCleanableRatio = 0.0;


    /**
     * @return the position processed for all logs.
     */
    public Map<TopicAndPartition, Long> allCleanerCheckpoints() throws IOException {
        Map<TopicAndPartition, Long> result = Maps.newHashMap();
        for (OffsetCheckpoint oc : checkpoints.values()) {
            result.putAll(oc.read());
        }
        return result;
    }

    /**
     * Choose the log to clean next and add it to the in-progress set. We recompute this
     * every time off the full set of logs to allow logs to be dynamically added to the pool of logs
     * the log manager maintains.
     */
    public Optional<LogToClean> grabFilthiestLog() {
        return Utils.inLock(lock, () -> {
//            try {
//                Map<TopicAndPartition, Long> lastClean = allCleanerCheckpoints();
//                List<LogToClean> dirtyLogs = logs.list().stream().filter(l -> l.v2.config.compact)          // skip any logs marked for delete rather than dedupe;
//                        .filter(l -> !inProgress.containsKey(l.v1)) // skip any logs already in-progress;
//                        .map(l -> new LogToClean(l.v1, l.v2,           // create a LogToClean instance for each;
//                                lastClean.getOrDefault(l.v1, l.v2.logSegments().stream().findFirst().get().baseOffset)))
//                        .filter(l -> l.totalBytes() > 0).collect(Collectors.toList());             // skip any empty logs;
//                if (!dirtyLogs.isEmpty()) {
//                    this.dirtiestLogCleanableRatio = Collections.max(dirtyLogs).cleanableRatio;
//                } else {
//                    this.dirtiestLogCleanableRatio = 0d;
//                }
//                List<LogToClean> cleanableLogs = dirtyLogs.stream().filter(l -> l.cleanableRatio > l.log.config.minCleanableRatio).collect(Collectors.toList()); // and must meet the minimum threshold for dirty byte ratio;
//                if (cleanableLogs.isEmpty()) {
//                    return Optional.empty();
//                } else {
//                    LogToClean filthiest = Collections.max(cleanableLogs);
//                    inProgress.put(filthiest.topicPartition, LogCleaningState.LogCleaningInProgress);
//                    return Optional.of(filthiest);
//                }
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
            return Optional.empty();
        });
    }

    /**
     * Abort the cleaning of a particular partition, if it's in progress. This call blocks until the cleaning of
     * the partition is aborted.
     * This is implemented by first abortAndPausing and then resuming the cleaning of the partition.
     */
    public void abortCleaning(TopicAndPartition topicAndPartition) {
        Utils.inLock(lock, () -> {
            abortAndPauseCleaning(topicAndPartition);
            resumeCleaning(topicAndPartition);
            info(String.format("The cleaning for partition %s is aborted", topicAndPartition));
        });
    }

    /**
     * Abort the cleaning of a particular partition if it's in progress, and pause any future cleaning of this partition.
     * This call blocks until the cleaning of the partition is aborted and paused.
     * 1. If the partition is not in progress, mark it as paused.
     * 2. Otherwise, first mark the state of the partition as aborted.
     * 3. The cleaner thread checks the state periodically and if it sees the state of the partition is aborted, it
     * throws a LogCleaningAbortedException to stop the cleaning task.
     * 4. When the cleaning task is stopped, doneCleaning() is called, which sets the state of the partition as paused.
     * 5. abortAndPauseCleaning() waits until the state of the partition is changed to paused.
     */
    public void abortAndPauseCleaning(TopicAndPartition topicAndPartition) {
        Utils.inLock(lock, () -> {
            LogCleaningState state = inProgress.get(topicAndPartition);
            if (state == null) {
                inProgress.put(topicAndPartition, LogCleaningState.LogCleaningPaused);
            } else {
                if (state == LogCleaningState.LogCleaningInProgress) {
                    inProgress.put(topicAndPartition, LogCleaningState.LogCleaningAborted);
                } else {
                    throw new IllegalStateException(String.format("Compaction for partition %s cannot be aborted and paused since it is in %s state.",
                            topicAndPartition, state));
                }
            }
            while (!isCleaningInState(topicAndPartition, LogCleaningState.LogCleaningPaused)) {
                try {
                    pausedCleaningCond.await(100, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            info(String.format("The cleaning for partition %s is aborted and paused", topicAndPartition));
        });
    }

    /**
     * Resume the cleaning of a paused partition. This call blocks until the cleaning of a partition is resumed.
     */
    public void resumeCleaning(TopicAndPartition topicAndPartition) {
        Utils.inLock(lock, () -> {
            LogCleaningState state = inProgress.get(topicAndPartition);
            if (state == null) {
                throw new IllegalStateException(String.format("Compaction for partition %s cannot be resumed since it is not paused.", topicAndPartition));
            } else {
                if (state == LogCleaningState.LogCleaningPaused) {
                    inProgress.remove(topicAndPartition);
                } else {
                    throw new IllegalStateException(String.format("Compaction for partition %s cannot be resumed since it is in %s state.", topicAndPartition, state));
                }
            }
        });
        info(String.format("Compaction for partition %s is resumed", topicAndPartition));
    }

    /**
     * Check if the cleaning for a partition is in a particular state. The caller is expected to hold lock while making the call.
     */
    public Boolean isCleaningInState(TopicAndPartition topicAndPartition, LogCleaningState expectedState) {
        LogCleaningState state = inProgress.get(topicAndPartition);
        if (state == expectedState) {
            return true;
        }
        return false;
    }

    /**
     * Check if the cleaning for a partition is aborted. If so, throw an exception.
     */
    public void checkCleaningAborted(TopicAndPartition topicAndPartition) {
        Utils.inLock(lock, () -> {
            if (isCleaningInState(topicAndPartition, LogCleaningState.LogCleaningAborted))
                throw new LogCleaningAbortedException();
        });
    }

    public void updateCheckpoints(File dataDir, Optional<Tuple<TopicAndPartition, Long>> update) {
        Utils.inLock(lock, () -> {
            OffsetCheckpoint checkpoint = checkpoints.get(dataDir);
            try {
                Map<TopicAndPartition, Long> existing = checkpoint.read();
                for (TopicAndPartition tp : existing.keySet()) {
                    if (!logs.keys().contains(tp)) {
                        existing.remove(tp);
                    }
                }
                if (update.isPresent()) {
                    existing.put(update.get().v1, update.get().v2);
                }
                checkpoint.write(existing);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    /**
     * Save out the endOffset and remove the given log from the in-progress set, if not aborted.
     */
    public void doneCleaning(TopicAndPartition topicAndPartition, File dataDir, Long endOffset) {
        Utils.inLock(lock, () -> {
            LogCleaningState state = inProgress.get(topicAndPartition);
            if (state == LogCleaningState.LogCleaningInProgress) {
                updateCheckpoints(dataDir, Optional.of(Tuple.of(topicAndPartition, endOffset)));
                inProgress.remove(topicAndPartition);
            } else if (state == LogCleaningState.LogCleaningAborted) {
                inProgress.put(topicAndPartition, LogCleaningState.LogCleaningPaused);
                pausedCleaningCond.signalAll();
            } else {
                throw new IllegalStateException(String.format("In-progress partition %s cannot be in %s state.", topicAndPartition, state));
            }
        });
    }
}

