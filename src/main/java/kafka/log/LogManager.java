package kafka.log;/**
 * Created by zhoulf on 2017/4/17.
 */

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import kafka.annotation.threadsafe;
import kafka.common.KafkaException;
import kafka.func.Action;
import kafka.func.Tuple;
import kafka.server.BrokerState;
import kafka.server.BrokerStates;
import kafka.utils.*;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
@threadsafe
public class LogManager extends Logging {
    public List<File> logDirs;
    public Map<String, LogConfig> topicConfigs;
    public LogConfig defaultConfig;
    public CleanerConfig cleanerConfig;
    public Integer ioThreads;
    public Long flushCheckMs;
    public Long flushCheckpointMs;
    public Long retentionCheckMs;
    public Scheduler scheduler;
    public BrokerState brokerState;
    private Time time;
    //////////////////////////////////////
    public String RecoveryPointCheckpointFile = "recovery-point-offset-checkpoint";
    public String LockFile = ".lock";
    public Long InitialTaskDelayMs = 30 * 1000L;
    private Object logCreationOrDeletionLock = new Object();
    private Pool<TopicAndPartition, Log> logs = new Pool<>();


    private List<FileLock> dirLocks;
    private Map<File, OffsetCheckpoint> recoveryPointCheckpoints;

    /**
     * The entry point to the kafka log management subsystem. The log manager is responsible for log creation, retrieval, and cleaning.
     * All read and write operations are delegated to the individual log instances.
     * <p>
     * The log manager maintains logs in one or more directories. New logs are created in the data directory
     * with the fewest logs. No attempt is made to move partitions after the fact or balance based on
     * size or I/O rate.
     * <p>
     * A background thread handles log retention by periodically truncating excess log segments.
     */
    public LogManager(File[] logDirs, Map<String, LogConfig> topicConfigs, LogConfig defaultConfig, CleanerConfig cleanerConfig, java.lang.Integer ioThreads, Long flushCheckMs, Long flushCheckpointMs, Long retentionCheckMs, Scheduler scheduler, BrokerState brokerState, Time time) {
        this.logDirs = Lists.newArrayList(logDirs);
        this.topicConfigs = topicConfigs;
        this.defaultConfig = defaultConfig;
        this.cleanerConfig = cleanerConfig;
        this.ioThreads = ioThreads;
        this.flushCheckMs = flushCheckMs;
        this.flushCheckpointMs = flushCheckpointMs;
        this.retentionCheckMs = retentionCheckMs;
        this.scheduler = scheduler;
        this.brokerState = brokerState;
        this.time = time;
        init();
    }

    public void init() {
        createAndValidateLogDirs(logDirs);
        dirLocks = lockLogDirs(logDirs);
        recoveryPointCheckpoints = Arrays.stream(logDirs).map(dir -> {
            try {
                return Tuple.of(dir, new OffsetCheckpoint(new File(dir, RecoveryPointCheckpointFile)));
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }).collect(Collectors.toMap(t -> t.v1, t -> t.v2));
        loadLogs();

        if (cleanerConfig.enableCleaner)
            cleaner = new LogCleaner(cleanerConfig, logDirs, logs, time = time);
    }


    // public, so we can access this from kafka.admin.DeleteTopicTest;
    LogCleaner cleaner = null;

    /**
     * Create and check validity of the given directories, specifically:
     * <ol>
     * <li> Ensure that there are no duplicates in the directory list
     * <li> Create each directory if it doesn't exist
     * <li> Check that each path is a readable directory
     * </ol>
     */
    private void createAndValidateLogDirs(List<File> dirs) {
        if (dirs.stream().map(d -> {
            try {
                return d.getCanonicalPath();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return "";
        }).collect(Collectors.toSet()).size() < dirs.size()) {
            throw new KafkaException("Duplicate log directory found: " + logDirs);
        }
        for (File dir : dirs) {
            if (!dir.exists()) {
                info("Log directory '" + dir.getAbsolutePath() + "' not found, creating it.");
                if (!dir.mkdirs())
                    throw new KafkaException("Failed to create data directory " + dir.getAbsolutePath());
            }
            if (!dir.isDirectory() || !dir.canRead())
                throw new KafkaException(dir.getAbsolutePath() + " is not a readable log directory.");
        }
    }

    /**
     * Lock all the given directories
     */
    private List<FileLock> lockLogDirs(File[] dirs) {
        return Arrays.stream(dirs).map(dir -> {
            FileLock lock = new FileLock(new File(dir, LockFile));
            if (!lock.tryLock())
                throw new KafkaException("Failed to acquire lock on file .lock in " + lock.file.getParentFile().getAbsolutePath() +
                        ". A Kafka instance in another process or thread is using this directory.");
            return lock;
        }).collect(Collectors.toList());
    }

    /**
     * Recover and load all logs in the given data directories
     * 加载目录下所有的topic-partition到 Pool logs<TopicAndPartition, Log>
     */
    private void loadLogs() throws Exception {
        info("Loading logs.");

        List<ExecutorService> threadPools = Lists.newArrayList();
        Map<File, Set<Future>> jobs = Maps.newHashMap();
        for (File dir : this.logDirs) {
            ExecutorService pool = Executors.newFixedThreadPool(ioThreads);
            threadPools.add(pool);

            File cleanShutdownFile = new File(dir, Log.CleanShutdownFile);

            if (cleanShutdownFile.exists()) {
                debug("Found clean shutdown file. " +
                        "Skipping recovery for all logs in data directory: " +
                        dir.getAbsolutePath());
            } else {
                // log recovery itself is being performed by `Log` class during initialization;
                brokerState.newState(BrokerStates.RecoveringFromUncleanShutdown);
            }

            Map<TopicAndPartition, Long> recoveryPoints = this.recoveryPointCheckpoints.get(dir).read();
            List<Runnable> jobsForDir = Lists.newArrayList();
            for (File dirContent : dir.listFiles()) {
                if (dirContent.isDirectory()) {
                    File logDir = dirContent;
                    jobsForDir.add(Utils.runnable(() -> {
                        debug("Loading log '" + logDir.getName() + "'");

                        TopicAndPartition topicPartition = Log.parseTopicPartitionName(logDir.getName());
                        LogConfig config = topicConfigs.getOrDefault(topicPartition.topic, defaultConfig);
                        Long logRecoveryPoint = recoveryPoints.getOrDefault(topicPartition, 0L);

                        Log current = new Log(logDir, config, logRecoveryPoint, scheduler, time);
                        Log previous = this.logs.put(topicPartition, current);

                        if (previous != null) {
                            throw new IllegalArgumentException(
                                    String.format("Duplicate log directories found: %s, %s!",
                                            current.dir.getAbsolutePath(), previous.dir.getAbsolutePath()));
                        }
                    }));
                }
            }

            jobs.put(cleanShutdownFile, jobsForDir.stream().map(job -> pool.submit(job)).collect(Collectors.toSet()));
        }


        try {
            for (Map.Entry<File, Set<Future>> entry : jobs.entrySet()) {
                File cleanShutdownFile = entry.getKey();
                Set<Future> dirJobs = entry.getValue();
                dirJobs.forEach(job -> {
                    try {
                        job.get();
                    } catch (InterruptedException e) {
                        error("There was an InterruptedException in one of the threads during logs loading: " + e.getCause());
                    } catch (ExecutionException e) {
                        error("There was an error in one of the threads during logs loading: " + e.getCause());
                    }
                });
                cleanShutdownFile.delete();
            }
        } finally {
            threadPools.forEach(t -> t.shutdown());
        }
        info("Logs loading complete.");
    }

    /**
     * Start the background threads to flush logs and do log cleanup
     * 启动定时任务
     * 1.日志保留（大小、保留时间）
     * 2.fileChannnel.flush()策略
     * 3.写入segment的recoverPointOffset
     * <p>
     * 启动cleaner
     */

    public void startup() {
    /* Schedule the cleanup task to delete old logs */
        if (scheduler != null) {
            info(String.format("Starting log cleanup with a period of %d ms.", retentionCheckMs));
            scheduler.schedule("kafka-log-retention",
                    cleanupLogs,
                    InitialTaskDelayMs,
                    retentionCheckMs,
                    TimeUnit.MILLISECONDS);
            info(String.format("Starting log flusher with a default period of %d ms.", flushCheckMs))
            scheduler.schedule("kafka-log-flusher",
                    flushDirtyLogs,
                    InitialTaskDelayMs,
                    flushCheckMs,
                    TimeUnit.MILLISECONDS);
            scheduler.schedule("kafka-recovery-point-checkpoint",
                    checkpointRecoveryPointOffsets,
                    InitialTaskDelayMs,
                    flushCheckpointMs,
                    TimeUnit.MILLISECONDS);
        }
        if (cleanerConfig.enableCleaner)
            cleaner.startup();
    }

    /**
     * Close all the logs
     */
    public void shutdown() {
        info("Shutting down.");

        val threadPools = mutable.ArrayBuffer.empty < ExecutorService >
                val jobs = mutable.Map.empty < File, Seq[Future[_]] >

        // stop the cleaner first;
        if (cleaner != null) {
            Utils.swallow(cleaner.shutdown());
        }

        // close logs in each dir;
        for (dir< -this.logDirs) {
            debug("Flushing and closing logs at " + dir);

            val pool = Executors.newFixedThreadPool(ioThreads);
            threadPools.append(pool);

            val logsInDir = logsByDir.getOrElse(dir.toString, Map()).values;

            val jobsForDir = logsInDir map {
                log =>
                Utils.runnable {
                    // flush the log to ensure latest possible recovery point;
                    log.flush();
                    log.close();
                }
            }

            jobs(dir) = jobsForDir.map(pool.submit).toSeq;
        }


        try {
            for ((dir, dirJobs)<-jobs){
                dirJobs.foreach(_.get)

                // update the last flush point;
                debug("Updating recovery points at " + dir);
                checkpointLogsInDir(dir);

                // mark that the shutdown was clean by creating marker file;
                debug("Writing clean shutdown marker at " + dir);
                Utils.swallow(new File(dir, Log.CleanShutdownFile).createNewFile());
            }
        } catch {
            case ExecutionException e =>{
                error("There was an error in one of the threads during LogManager shutdown: " + e.getCause);
                throw e.getCause;
            }
        }finally{
            threadPools.foreach(_.shutdown())
            // regardless of whether the close succeeded, we need to unlock the data directories;
            dirLocks.foreach(_.destroy())
        }

        info("Shutdown complete.");
    }


    /**
     * Truncate the partition logs to the specified offsets and checkpoint the recovery point to this offset
     *
     * @param partitionAndOffsets Partition logs that need to be truncated
     */
    public void truncateTo(Map partitionAndOffsets<TopicAndPartition, Long>) {
        for ((topicAndPartition, truncateOffset)<-partitionAndOffsets){
            val log = logs.get(topicAndPartition);
            // If the log does not exist, skip it;
            if (log != null) {
                //May need to abort and pause the cleaning of the log, and resume after truncation is done.;
                val Boolean needToStopCleaner = (truncateOffset < log.activeSegment.baseOffset);
                if (needToStopCleaner && cleaner != null)
                    cleaner.abortAndPauseCleaning(topicAndPartition);
                log.truncateTo(truncateOffset);
                if (needToStopCleaner && cleaner != null)
                    cleaner.resumeCleaning(topicAndPartition);
            }
        }
        checkpointRecoveryPointOffsets();
    }

    /**
     * Delete all data in a partition and start the log at the new offset
     *
     * @param newOffset The new offset to start the log with
     */
    public void truncateFullyAndStartAt(TopicAndPartition topicAndPartition, Long newOffset) {
        val log = logs.get(topicAndPartition);
        // If the log does not exist, skip it;
        if (log != null) {
            //Abort and pause the cleaning of the log, and resume after truncation is done.;
            if (cleaner != null)
                cleaner.abortAndPauseCleaning(topicAndPartition);
            log.truncateFullyAndStartAt(newOffset);
            if (cleaner != null)
                cleaner.resumeCleaning(topicAndPartition);
        }
        checkpointRecoveryPointOffsets();
    }

    /**
     * Write out the current recovery point for all logs to a text file in the log directory
     * to avoid recovering the whole log on startup.
     */
    public Action checkpointRecoveryPointOffsets = () -> Arrays.stream(this.logDirs).forEach(l -> checkpointLogsInDir);

    /**
     * Make a checkpoint for all logs in provided directory.
     */
    private void checkpointLogsInDir(File dir) {
        List<Tuple<TopicAndPartition, Log>> recoveryPoints = this.logsByDir().get(dir.toString());
        if (recoveryPoints != null) {
            this.recoveryPointCheckpoints(dir).write(recoveryPoints.get.mapValues(_.recoveryPoint));
        }
    }

    /**
     * Get the log if it exists, otherwise return None
     */
    public Optional<Log> getLog(TopicAndPartition topicAndPartition) {
        Log log = logs.get(topicAndPartition);
        if (log == null)
            return Optional.empty();
        else
            return Optional.of(log);
    }

/**
 * Create a log for the given topic and the given partition
 * If the log already exists, just return a copy of the existing log
 */
    public Log createLog(TopicAndPartition topicAndPartition, LogConfig config) {
         synchronized(logCreationOrDeletionLock) {
            Log log = logs.get(topicAndPartition);

            // check if the log has already been created in another thread;
            if (log != null)
                return log;

            // if not, create it;
            File dataDir = nextLogDir();
            val dir = new File(dataDir, topicAndPartition.topic + "-" + topicAndPartition.partition);
            dir.mkdirs();
            log = new Log(dir,
                    config,
                    recoveryPoint = 0L,
                    scheduler,
                    time);
            logs.put(topicAndPartition, log);
            info("Created log for partition <%s,%d> in %s with properties {%s}.";
            .format(topicAndPartition.topic,
                    topicAndPartition.partition,
                    dataDir.getAbsolutePath,
                    {import JavaConversions._;
            config.toProps.mkString(", ")}));
            log;
        }
    }

    /**
     * Delete a log.
     */
    public void deleteLog(TopicAndPartition topicAndPartition) {
        var Log removedLog = null;
        logCreationOrDeletionLock synchronized {
            removedLog = logs.remove(topicAndPartition);
        }
        if (removedLog != null) {
            //We need to wait until there is no more cleaning task on the log to be deleted before actually deleting it.;
            if (cleaner != null) {
                cleaner.abortCleaning(topicAndPartition);
                cleaner.updateCheckpoints(removedLog.dir.getParentFile);
            }
            removedLog.delete();
            info("Deleted log for partition <%s,%d> in %s.";
            .format(topicAndPartition.topic,
                    topicAndPartition.partition,
                    removedLog.dir.getAbsolutePath));
        }
    }

    /**
     * Choose the next directory in which to create a log. Currently this is done
     * by calculating the number of partitions in each directory and then choosing the
     * data directory with the fewest partitions.
     */
    private File nextLogDir() {
        if (logDirs.length == 1) {
            return logDirs[0];
        } else {
            // count the number of logs in each parent directory (including 0 for empty directories;
            val logCounts = allLogs().groupBy(_.dir.getParent).mapValues(_.size);
            val zeros = logDirs.map(dir = > (dir.getPath, 0)).toMap;
            var dirCounts = (zeros++ logCounts).toBuffer;

            // choose the directory with the least logs in it;
            val leastLoaded = dirCounts.sortBy(_._2).head;
            new File(leastLoaded._1);
        }
    }

    /**
     * Runs through the log removing segments older than a certain age
     */
    private Integer cleanupExpiredSegments(Log log) throws IOException {
        Long startMs = time.milliseconds();
        return log.deleteOldSegments(l -> startMs - l.lastModified() > log.config.retentionMs);
    }

    /**
     * Runs through the log removing segments until the size of the log
     * is at least logRetentionSize bytes in size
     */
    private Integer cleanupSegmentsToMaintainSize(Log log) {
        if (log.config.retentionSize < 0 || log.size() < log.config.retentionSize)
            return 0;
        final Long diff = log.size() - log.config.retentionSize;
        return log.deleteOldSegments((segment) -> {
            if (diff - segment.size() >= 0) {
                diff -= segment.size();
                return true;
            } else {
                return false;
            }
        });
    }


    /**
     * Delete any eligible logs. Return the number of segments deleted.
     */
    public Action cleanupLogs = () -> {
        debug("Beginning log cleanup...");
        Integer total = 0;
        Long startMs = time.milliseconds();
        for (Log log : allLogs()) {
            if (!log.config.compact) {
                debug("Garbage collecting '" + log.name + "'");
                total += cleanupExpiredSegments(log) + cleanupSegmentsToMaintainSize(log);
            }
            debug("Log cleanup completed. " + total + " files deleted in " + (time.milliseconds() - startMs) / 1000 + " seconds");
        }
    };

    /**
     * Get all the partition logs
     */
    public Iterable<Log> allLogs() {
        return logs.values();
    }

    /**
     * Get a map of TopicAndPartition => Log
     */
    public Map<TopicAndPartition, Log> logsByTopicPartition = logs.toMap();

    /**
     * Map of log dir to logs by topic and partitions in that dir
     */
    private Map<String, List<Tuple<TopicAndPartition, Log>>> logsByDir() {
        List<Tuple<String, Tuple<TopicAndPartition, Log>>> list = this.logsByTopicPartition.entrySet().stream()
                .map(entry -> Tuple.of(entry.getValue().dir.getParent(), Tuple.of(entry.getKey(), entry.getValue()))).collect(Collectors.toList());
        Map<String, List<Tuple<TopicAndPartition, Log>>> map = Maps.newHashMap();
        for (Tuple<String, Tuple<TopicAndPartition, Log>> t : list) {
            List<Tuple<TopicAndPartition, Log>> l = map.getOrDefault(t.v1, Lists.newArrayList());
            map.put(t.v1, l);
        }
        return map;
    }

    /**
     * Flush any log which has exceeded its flush interval and has unwritten messages.
     */
    private Action flushDirtyLogs = () -> {
        debug("Checking for dirty logs to flush...");

        for (Tuple<TopicAndPartition, Log> t : logs) {
            TopicAndPartition topicAndPartition = t.v1;
            Log log = t.v2;
            try {
                Long timeSinceLastFlush = time.milliseconds() - log.lastFlushTime();
                debug("Checking if flush is needed on " + topicAndPartition.topic + " flush interval  " + log.config.flushMs +
                        " last flushed " + log.lastFlushTime() + " time since last flush: " + timeSinceLastFlush);
                if (timeSinceLastFlush >= log.config.flushMs)
                    log.flush();
            } catch (Exception e) {
                error("Error flushing topic " + topicAndPartition.topic, e);
            }
        }
    };

}
