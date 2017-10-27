package kafka.log;/**
 * Created by zhoulf on 2017/3/29.
 */


import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.yammer.metrics.core.Gauge;
import kafka.annotation.threadsafe;
import kafka.common.*;
import kafka.func.Action;
import kafka.func.Handler;
import kafka.message.*;
import kafka.metrics.KafkaMetricsGroup;
import kafka.server.BrokerTopicStats;
import kafka.server.FetchDataInfo;
import kafka.server.LogOffsetMetadata;
import kafka.utils.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.NumberFormat;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;


/**
 * An append-only log for storing messages.
 * <p>
 * The log is a sequence of LogSegments, each with a base offset denoting the first message in the segment.
 * <p>
 * New log segments are created according to a configurable policy that controls the size in bytes or time interval
 * for a given segment.
 */
@threadsafe
public class Log extends KafkaMetricsGroup {
    public File dir;
    public volatile LogConfig config;
    public volatile Long recoveryPoint = 0L;
    public Scheduler scheduler;
    public Time time;

    /**
     * @param dir           The directory in which log segments are created.
     * @param config        The log configuration settings
     * @param recoveryPoint The offset at which to begin recovery--i.e. the first offset which has not been flushed to disk
     * @param scheduler     The thread pool scheduler used for background actions
     * @param time          The time instance used for checking the clock
     */
    public Log(File dir, LogConfig config, Long recoveryPoint, Scheduler scheduler, Time time) throws IOException {
        this.dir = dir;
        this.config = config;
        this.recoveryPoint = recoveryPoint;
        this.scheduler = scheduler;
        this.time = time;
        init();
    }

    /* A lock that guards all modifications to the log */
    private Object lock = new Object();

    /* last time it was flushed */
    private AtomicLong lastflushedTime ;

    /* the actual segments of the log */
    private ConcurrentNavigableMap<Long, LogSegment> segments = new ConcurrentSkipListMap<Long, LogSegment>();
    /* Calculate the offset of the next message */
    public volatile LogOffsetMetadata nextOffsetMetadata;
    TopicAndPartition topicAndPartition;
    Map<String, String> tags;
    /**
     * The name of this log
     */
    public String name;

    public void init() throws IOException {
        lastflushedTime = new AtomicLong(time.milliseconds());

        //load
        loadSegments();

        name = dir.getName();
        nextOffsetMetadata = new LogOffsetMetadata(activeSegment().nextOffset(), activeSegment().baseOffset, activeSegment().size().intValue());

        topicAndPartition = Log.parseTopicPartitionName(name);

        info(String.format("Completed load of log %s with log end offset %d", name, logEndOffset()));

        tags = Maps.newHashMap();
        tags.put("topic", topicAndPartition.topic);
        tags.put("partition", topicAndPartition.partition.toString());
        newGuages();
    }


    /* Load the log segments from the log files on disk */
    private void loadSegments() throws IOException {
        // create the log directory if it doesn't exist;
        dir.mkdirs();

        // first do a pass through the files in the log directory and remove any temporary files;
        // and complete any interrupted swap operations;
        for (File file : dir.listFiles()) {
            if (!file.isFile()) {
                continue;
            }
            if (!file.canRead())
                throw new IOException("Could not read file " + file);
            String filename = file.getName();
            if (filename.endsWith(DeletedFileSuffix) || filename.endsWith(CleanedFileSuffix)) {
                // if the file ends in .deleted or .cleaned, delete it;
                file.delete();
            } else if (filename.endsWith(SwapFileSuffix)) {
                // we crashed in the middle of a swap operation, to recover:;
                // if a log, swap it in and delete the .index file;
                // if an index just delete it, it will be rebuilt;
                File baseName = new File(Utils.replaceSuffix(file.getPath(), SwapFileSuffix, ""));
                if (baseName.getPath().endsWith(IndexFileSuffix)) {
                    file.delete();
                } else if (baseName.getPath().endsWith(LogFileSuffix)) {
                    // delete the index;
                    File index = new File(Utils.replaceSuffix(baseName.getPath(), LogFileSuffix, IndexFileSuffix));
                    index.delete();
                    // complete the swap operation;
                    boolean renamed = file.renameTo(baseName);
                    if (renamed)
                        info(String.format("Found log file %s from interrupted swap operation, repairing.", file.getPath()));
                    else
                        throw new KafkaException(String.format("Failed to rename file %s.", file.getPath()));
                }
            }
        }

        // now do a second pass and load all the .log and .index files;
        for (File file : dir.listFiles()) {
            if (!file.isFile()) {
                continue;
            }
            String filename = file.getName();
            if (filename.endsWith(IndexFileSuffix)) {
                // if it is an index file, make sure it has a corresponding .log file;
                File logFile = new File(file.getAbsolutePath().replace(IndexFileSuffix, LogFileSuffix));
                if (!logFile.exists()) {
                    warn(String.format("Found an orphaned index file, %s, with no corresponding log file.", file.getAbsolutePath()));
                    file.delete();
                }
            } else if (filename.endsWith(LogFileSuffix)) {
                // if its a log file, load the corresponding log segment;
                Long start = Long.parseLong(filename.substring(0, filename.length() - LogFileSuffix.length()));
                boolean hasIndex = Log.indexFilename(dir, start).exists();
                LogSegment segment = new LogSegment(dir, start, config.indexInterval, config.maxIndexSize, config.randomSegmentJitter, time);
                if (!hasIndex) {
                    error(String.format("Could not find index file corresponding to log file %s, rebuilding index...", segment.log.file.getAbsolutePath()));
                    segment.recover(config.maxMessageSize);
                }
                segments.put(start, segment);
            }
        }
        if (logSegments().size() == 0) {
            // no existing segments, create a new mutable segment beginning at offset 0;
            segments.put(0L, new LogSegment(dir, 0L, config.indexInterval, config.maxIndexSize, config.randomSegmentJitter, time));
        } else {
            recoverLog();
            // reset the index size of the currently active log segment to allow more entries;
            activeSegment().index.resize(config.maxIndexSize);
        }

        // sanity check the index file of every segment to ensure we don't proceed with a corrupt segment;
        for (LogSegment s : logSegments())
            s.index.sanityCheck();
    }

    private void updateLogEndOffset(Long messageOffset) {
        nextOffsetMetadata = new LogOffsetMetadata(messageOffset, activeSegment().baseOffset, activeSegment().size().intValue());
    }

    private void recoverLog() throws IOException {
        // if we have the clean shutdown marker, skip recovery;
        if (hasCleanShutdownFile()) {
            this.recoveryPoint = activeSegment().nextOffset();
            return;
        }

        // okay we need to actually recovery this log;
        Iterator<LogSegment> unflushed = logSegments(this.recoveryPoint, Long.MAX_VALUE).iterator();
        while (unflushed.hasNext()) {
            LogSegment curr = unflushed.next();
            info(String.format("Recovering unflushed segment %d in log %s.", curr.baseOffset, name));
            Integer truncatedBytes;
            try {
                truncatedBytes = curr.recover(config.maxMessageSize);
            } catch (InvalidOffsetException e) {
                Long startOffset = curr.baseOffset;
                warn("Found invalid offset during recovery for log " + dir.getName() + ". Deleting the corrupt segment and " +
                        "creating an empty one with starting offset " + startOffset);
                truncatedBytes = curr.truncateTo(startOffset);
            }
            if (truncatedBytes > 0) {
                // we had an invalid message, delete all remaining log;
                warn(String.format("Corruption found in segment %d of log %s, truncating to offset %d.", curr.baseOffset, name, curr.nextOffset()));
                unflushed.forEachRemaining(s -> deleteSegment(s));
                unflushed.forEachRemaining(s -> deleteSegment(s));
            }
        }
    }

    /**
     * Check if we have the "clean shutdown" file
     */
    private boolean hasCleanShutdownFile() {
        return new File(dir.getParentFile(), CleanShutdownFile).exists();
    }

    /**
     * The number of segments in the log.
     * Take care! this is an O(n) operation.
     */
    public Integer numberOfSegments() {
        return segments.size();
    }

    /**
     * Close this log
     */
    public void close() {
        debug("Closing log " + name);
        synchronized (lock) {
            logSegments().forEach((seg) -> seg.close());
        }
    }

    public LogAppendInfo append(ByteBufferMessageSet messages) {
        return append(messages, true);
    }

    /**
     * Append this message set to the active segment of the log, rolling over to a fresh segment if necessary.
     * <p>
     * This method will generally be responsible for assigning offsets to the messages,
     * however if the assignOffsets=false flag is passed we will only check that the existing offsets are valid.
     *
     * @param messages      The message set to append
     * @param assignOffsets Should the log assign offsets to this message set or blindly apply what it is given
     * @return Information about the appended messages including the first and last offset.
     * @throws KafkaStorageException If the append fails due to an I/O error.
     * 1.分析成LogAppendInfo
     * 2.检查有效bytes
     * 3.自增offset(nextOffset)
     * 4.roll()校验
     * 5.segment.append()
     * 6.更新logEndOffset
     * 7.刷新间隔offset
     */
    public LogAppendInfo append(ByteBufferMessageSet messages, boolean assignOffsets) {
        LogAppendInfo appendInfo = analyzeAndValidateMessageSet(messages);//生成LogAppendInfo;

        // if we have any valid messages, append them to the log;
        if (appendInfo.shallowCount == 0)
            return appendInfo;

        // trim any invalid bytes or partial messages before appending it to the on-disk log;
        ByteBufferMessageSet validMessages = trimInvalidBytes(messages, appendInfo);//过滤有效字符;

        try {
            // they are valid, insert them in the log;
            synchronized (lock) {
                appendInfo.firstOffset = nextOffsetMetadata.messageOffset;

                if (assignOffsets) {//默认offset自增
                    // assign offsets to the message set;
                    AtomicLong offset = new AtomicLong(nextOffsetMetadata.messageOffset);
                    try {
                        validMessages = validMessages.assignOffsets(offset, appendInfo.codec);
                    } catch (Exception e) {
                        throw new KafkaException(String.format("Error in validating messages while appending to log '%s'", name), e);
                    }
                    appendInfo.lastOffset = offset.get() - 1;
                } else {//非自增，校验是否小于之前的offset
                    // we are taking the offsets we are given;
                    if (!appendInfo.offsetsMonotonic || appendInfo.firstOffset < nextOffsetMetadata.messageOffset)
                        throw new IllegalArgumentException("Out of order offsets found in " + messages);
                }

                // re-validate message sizes since after re-compression some may exceed the limit;
                Sc.loop(validMessages.shallowIterator(), messageAndOffset -> {//浅遍历 消息长度是否超过maxMessageSize
                    if (MessageSet.entrySize(messageAndOffset.message) > config.maxMessageSize) {
                        // we record the original message set size instead of trimmed size;
                        // to be consistent with pre-compression bytesRejectedRate recording;
                        BrokerTopicStats.getBrokerTopicStats(topicAndPartition.topic).bytesRejectedRate.mark(messages.sizeInBytes());
                        BrokerTopicStats.getBrokerAllTopicsStats().bytesRejectedRate.mark(messages.sizeInBytes());
                        throw new MessageSizeTooLargeException(String.format("Message size is %d bytes which exceeds the maximum configured message size of %d.",
                                MessageSet.entrySize(messageAndOffset.message), config.maxMessageSize));
                    }
                });

                // check messages set size may be exceed config.segmentSize; 整条消息有效长度是否超过segmentSize
                if (validMessages.sizeInBytes() > config.segmentSize) {
                    throw new MessageSetSizeTooLargeException(String.format("Message set size is %d bytes which exceeds the maximum configured segment size of %d.",
                            validMessages.sizeInBytes(), config.segmentSize));
                }

                // maybe roll the log if this segment is full;
                LogSegment segment = maybeRoll(validMessages.sizeInBytes());

                // now append to the log;
                segment.append(appendInfo.firstOffset, validMessages);

                // increment the log end offset; 更新下个offset
                updateLogEndOffset(appendInfo.lastOffset + 1);

                trace(String.format("Appended message set to log %s with first offset: %d, next offset: %d, and messages: %s", this.name, appendInfo.firstOffset, nextOffsetMetadata.messageOffset, validMessages));

                if (unflushedMessages() >= config.flushInterval) {//刷新间隔offset
                    flush();
                }
                return appendInfo;
            }
        } catch (IOException e) {
            throw new KafkaStorageException(String.format("I/O exception in append to log '%s'", name), e);
        }
    }

    /**
     * Validate the following:
     * <ol>
     * <li> each message matches its CRC
     * <li> each message size is valid
     * </ol>
     * <p>
     * Also compute the following quantities:
     * <ol>
     * <li> First offset in the message set
     * <li> Last offset in the message set
     * <li> Number of messages
     * <li> Number of valid bytes
     * <li> Whether the offsets are monotonically increasing
     * <li> Whether any compression codec is used (if many are used, then the last one is given)
     * </ol>
     */
    private LogAppendInfo analyzeAndValidateMessageSet(ByteBufferMessageSet messages) {
        Integer shallowMessageCount = 0;
        Integer validBytesCount = 0;
        Long firstOffset = 0L, lastOffset = -1L;
        CompressionCodec codec = CompressionCodec.NoCompressionCodec;
        boolean monotonic = true;//是否升序

        Iterator<MessageAndOffset> it = messages.shallowIterator();
        while (it.hasNext()) {
            MessageAndOffset messageAndOffset = it.next();
            // update the first offset if on the first message;
            if (firstOffset < 0)
                firstOffset = messageAndOffset.offset;
            // check that offsets are monotonically increasing;
            if (lastOffset >= messageAndOffset.offset)
                monotonic = false;
            // update the last offset seen;
            lastOffset = messageAndOffset.offset;

            Message m = messageAndOffset.message;

            // Check if the message sizes are valid.;
            Integer messageSize = MessageSet.entrySize(m);
            if (messageSize > config.maxMessageSize) {
                BrokerTopicStats.getBrokerTopicStats(topicAndPartition.topic).bytesRejectedRate.mark(messages.sizeInBytes());
                BrokerTopicStats.getBrokerAllTopicsStats().bytesRejectedRate.mark(messages.sizeInBytes());
                throw new MessageSizeTooLargeException(String.format("Message size is %d bytes which exceeds the maximum configured message size of %d.", messageSize, config.maxMessageSize));
            }

            // check the validity of the message by checking CRC;
            m.ensureValid();

            shallowMessageCount += 1;
            validBytesCount += messageSize;

            CompressionCodec messageCodec = m.compressionCodec();
            if (messageCodec != CompressionCodec.NoCompressionCodec)
                codec = messageCodec;
        }
        return new LogAppendInfo(firstOffset, lastOffset, codec, shallowMessageCount, validBytesCount, monotonic);
    }

    /**
     * Trim any invalid bytes from the end of this message set (if there are any)
     *
     * @param messages The message set to trim
     * @param info     The general information of the message set
     * @return A trimmed message set. This may be the same as what was passed in or it may not.
     */
    private ByteBufferMessageSet trimInvalidBytes(ByteBufferMessageSet messages, LogAppendInfo info) {
        Integer messageSetValidBytes = info.validBytes;
        if (messageSetValidBytes < 0)
            throw new InvalidMessageSizeException("Illegal length of message set " + messageSetValidBytes + " Message set cannot be appended to log. Possible causes are corrupted produce requests");
        if (messageSetValidBytes == messages.sizeInBytes()) {
            return messages;
        } else {
            // trim invalid bytes;
            ByteBuffer validByteBuffer = messages.buffer.duplicate();
            validByteBuffer.limit(messageSetValidBytes);
            return new ByteBufferMessageSet(validByteBuffer);
        }
    }

    /**
     * Read messages from the log
     *
     * @param startOffset The offset to begin reading at
     * @param maxLength   The maximum number of bytes to read
     * @param maxOffset   -The offset to read up to, exclusive. (i.e. the first offset NOT included in the resulting message set).
     * @return The fetch data information including fetch starting offset metadata and messages read
     * @throws OffsetOutOfRangeException If startOffset is beyond the log end offset or before the base offset of the first segment.
     */
    public FetchDataInfo read(Long startOffset, Integer maxLength, Optional<Long> maxOffset) {
        trace(String.format("Reading %d bytes from offset %d in log %s of length %d bytes", maxLength, startOffset, name, size()));

        // check if the offset is valid and in range;
        Long next = nextOffsetMetadata.messageOffset;
        if (startOffset.equals(next)) {
            return new FetchDataInfo(nextOffsetMetadata, MessageSet.Empty);
        }

        Map.Entry<Long, LogSegment> entry = segments.floorEntry(startOffset);

        // attempt to read beyond the log end offset is an error;
        if (startOffset > next || entry == null) {
            throw new OffsetOutOfRangeException(String.format("Request for offset %d but we only have log segments in the range %d to %d.", startOffset, segments.firstKey(), next));
        }

        // do the read on the segment with a base offset less than the target offset;
        // but if that segment doesn't contain any messages with an offset greater than that;
        // continue to read from successive segments until we get some messages or we reach the end of the log;
        while (entry != null) {
            FetchDataInfo fetchInfo = entry.getValue().read(startOffset, maxOffset, maxLength);
            if (fetchInfo == null) {
                entry = segments.higherEntry(entry.getKey());
            } else {
                return fetchInfo;
            }
        }

        // okay we are beyond the end of the last segment with no data fetched although the start offset is in range,
        // this can happen when all messages with offset larger than start offsets have been deleted.;
        // In this case, we will return the empty set with log end offset metadata;
        return new FetchDataInfo(nextOffsetMetadata, MessageSet.Empty);
    }

    public FetchDataInfo read(Long startOffset, Integer maxLength) {
        return read(startOffset, maxLength, Optional.empty());
    }

    /**
     * Given a message offset, find its corresponding offset metadata in the log.
     * If the message offset is out of range, return unknown offset metadata
     */
    public LogOffsetMetadata convertToOffsetMetadata(Long offset) {
        try {
            FetchDataInfo fetchDataInfo = read(offset, 1, Optional.empty());
            return fetchDataInfo.fetchOffset;
        } catch (OffsetOutOfRangeException e) {
            return LogOffsetMetadata.UnknownOffsetMetadata;
        }
    }

    /**
     * Delete any log segments matching the given predicate function,
     * starting with the oldest segment and moving forward until a segment doesn't match.
     *
     * @param predicate A function that takes in a single log segment and returns true iff it is deletable
     * @return The number of segments deleted
     */
    public Integer deleteOldSegments(Handler<LogSegment, Boolean> predicate) throws IOException {
        // find any segments that match the user-supplied predicate UNLESS it is the final segment;
        // and it is empty (since we would just end up re-creating it;
        LogSegment lastSegment = activeSegment();
        // TODO: 2017/4/3 takewhile filter
//        val deletable = logSegments.takeWhile(s => predicate(s) && (s.baseOffset != lastSegment.baseOffset || s.size > 0))
        List<LogSegment> deletable = logSegments().stream().filter(s -> predicate.handle(s) && (s.baseOffset != lastSegment.baseOffset || s.size() > 0)).collect(Collectors.toList());
        Integer numToDelete = deletable.size();
        if (numToDelete > 0) {
            synchronized (lock) {
                // we must always have at least one segment, so if we are going to delete all the segments, create a new one first;
                if (segments.size() == numToDelete)
                    roll();
                // remove the segments for lookups;
                deletable.forEach(s -> deleteSegment(s));
            }
        }
        return numToDelete;
    }


    /**
     * The size of the log in bytes
     */
    public Long size() {
        Long sum = 0L;
        for (LogSegment s : logSegments()) {
            sum += s.size();
        }
        return sum;
    }

    /**
     * The earliest message offset in the log
     */
    public Long logStartOffset() {
        return logSegments().stream().findFirst().get().baseOffset;
    }

    /**
     * The offset metadata of the next message that will be appended to the log
     */
    public LogOffsetMetadata logEndOffsetMetadata() {
        return nextOffsetMetadata;
    }

    /**
     * The offset of the next message that will be appended to the log
     */
    public Long logEndOffset() {
        return nextOffsetMetadata.messageOffset;
    }

    /**
     * Roll the log over to a new empty log segment if necessary.
     *
     * @param messagesSize The messages set size in bytes
     *                     logSegment will be rolled if one of the following conditions met
     *                     <ol>
     *                     <li> The logSegment is full
     *                     <li> The maxTime has elapsed
     *                     <li> The index is full
     *                     </ol>
     * @return The currently active segment after (perhaps) rolling to a new segment
     */
    private LogSegment maybeRoll(Integer messagesSize) throws IOException {
        LogSegment segment = activeSegment();
        if (segment.size() + messagesSize > config.segmentSize//长度超过、时间超过、已满->roll()创建新年segment
                || (segment.size() > 0 && time.milliseconds() - segment.created > config.segmentMs - segment.rollJitterMs)
                || segment.index.isFull()) {
            debug(String.format("Rolling new log segment in %s (log_size = %d/%d, index_size = %d/%d, age_ms = %d/%d).",
                    name,
                    segment.size(),
                    config.segmentSize,
                    segment.index.entries(),
                    segment.index.maxEntries,
                    time.milliseconds() - segment.created,
                    config.segmentMs - segment.rollJitterMs));
            return roll();
        } else {
            return segment;
        }
    }

    /**
     * Roll the log over to a new active segment starting with the current logEndOffset.
     * This will trim the index to the exact size of the number of entries it currently contains.
     *  获取最新的offset+1 作为新日志的baseOffset 创建新日志文件、索引文件
     * @return The newly rolled segment
     */
    public LogSegment roll() throws IOException {
        Long start = time.nanoseconds();
        synchronized (lock) {
            Long newOffset = logEndOffset();
            File logFile = logFilename(dir, newOffset);
            File indexFile = indexFilename(dir, newOffset);
            //删除已经存在的文件
            Lists.newArrayList(logFile, indexFile).stream().filter(f -> f.exists())
                    .forEach(f -> {
                                warn("Newly rolled segment file " + f.getName() + " already exists; deleting it first");
                                f.delete();
                            });

            Map.Entry<Long, LogSegment> lastEntry = segments.lastEntry();
            if (lastEntry != null) {
                lastEntry.getValue().index.trimToValidSize();
            }
            LogSegment segment = new LogSegment(dir, newOffset, config.indexInterval, config.maxIndexSize, config.randomSegmentJitter, time);
            LogSegment prev = addSegment(segment);
            if (prev != null) {
                throw new KafkaException(String.format("Trying to roll a new log segment for topic partition %s with start offset %d while it already exists.", name, newOffset));
            }

            // schedule an asynchronous flush of the old segment; flush到磁盘
            scheduler.schedule("flush-log", () -> flush(newOffset), 0L);

            info(String.format("Rolled new log segment for '" + name + "' in %.0f ms.", (System.nanoTime() - start) / (1000.0 * 1000.0)));

            return segment;
        }
    }

    /**
     * The number of messages appended to the log since the last flush
     */

    public Long unflushedMessages() {
        return this.logEndOffset() - this.recoveryPoint;
    }

    /**
     * Flush all log segments
     */
    public void flush() {
        flush(this.logEndOffset());
    }

    /**
     * Flush log segments for all offsets up to offset-1
     *
     * @param offset The offset to flush up to (non-inclusive); the new recovery point
     */
    public void flush(Long offset) {
        if (offset <= this.recoveryPoint) {
            return;
        }
        debug("Flushing log '" + name + " up to offset " + offset + ", last flushed: " + lastFlushTime() + " current time: " + time.milliseconds() + " unflushed = " + unflushedMessages());
        logSegments(this.recoveryPoint, offset).forEach(s -> s.flush());
        synchronized (lock) {
            if (offset > this.recoveryPoint) {
                this.recoveryPoint = offset;
                lastflushedTime.set(time.milliseconds());
            }
        }
    }

    /**
     * Completely delete this log directory and all contents from the file system with no delay
     */
    void delete() {
        synchronized (lock) {
            logSegments().forEach((s) -> s.delete());
            segments.clear();
            Utils.rm(dir);
        }
    }

    /**
     * Truncate this log so that it ends with the greatest offset < targetOffset.
     *
     * @param targetOffset The offset to truncate to, an upper bound on all offsets in the log after truncation is complete.
     */
    void truncateTo(Long targetOffset) throws IOException {
        info(String.format("Truncating log %s to offset %d.", name, targetOffset));
        if (targetOffset < 0)
            throw new IllegalArgumentException(String.format("Cannot truncate to a negative offset (%d).", targetOffset));
        if (targetOffset > logEndOffset()) {
            info(String.format("Truncating %s to %d has no effect as the largest offset in the log is %d.", name, targetOffset, logEndOffset() - 1));
            return;
        }
        synchronized (lock) {
            if (segments.firstEntry().getValue().baseOffset > targetOffset) {
                truncateFullyAndStartAt(targetOffset);
            } else {
                List<LogSegment> deletable = logSegments().stream().filter(segment -> segment.baseOffset > targetOffset).collect(Collectors.toList());
                deletable.forEach(s -> deleteSegment(s));
                activeSegment().truncateTo(targetOffset);
                updateLogEndOffset(targetOffset);
                this.recoveryPoint = Math.min(targetOffset, this.recoveryPoint);
            }
        }
    }

    /**
     * Delete all data in the log and start at the new offset
     *
     * @param newOffset The new offset to start the log with
     */
    void truncateFullyAndStartAt(Long newOffset) throws IOException {
        debug("Truncate and start log '" + name + "' to " + newOffset);
        synchronized (lock) {
            logSegments().forEach(s -> deleteSegment(s));
            addSegment(new LogSegment(dir,
                    newOffset,
                    config.indexInterval,
                    config.maxIndexSize,
                    config.randomSegmentJitter,
                    time));
            updateLogEndOffset(newOffset);
            this.recoveryPoint = Math.min(newOffset, this.recoveryPoint);
        }
    }


    /**
     * The time this log is last known to have been fully flushed to disk
     */
    public Long lastFlushTime() {
        return lastflushedTime.get();
    }


    /**
     * The active segment that is currently taking appends
     */
    public LogSegment activeSegment() {
        return segments.lastEntry().getValue();
    }

    /**
     * All the log segments in this log ordered from oldest to newest
     */
    public Collection<LogSegment> logSegments() {
        return segments.values();
    }

    /**
     * Get all segments beginning with the segment that includes "from" and ending with the segment
     * that includes up to "to-1" or the end of the log (if to > logEndOffset)
     */
    public Collection<LogSegment> logSegments(Long from, Long to) {
        synchronized (lock) {
            Long floor = segments.floorKey(from);
            if (floor == null) {
                return segments.headMap(to).values();
            } else {
                return segments.subMap(floor, true, to, false).values();
            }
        }
    }

    @Override
    public String toString() {
        return "Log(" + dir + ")";
    }

    /**
     * This method performs an asynchronous log segment delete by doing the following:
     * <ol>
     * <li>It removes the segment from the segment map so that it will no longer be used for reads.
     * <li>It renames the index and log files by appending .deleted to the respective file name
     * <li>It schedules an asynchronous delete operation to occur in the future
     * </ol>
     * This allows reads to happen concurrently without synchronization and without the possibility of physically
     * deleting a file while it is being read from.
     *
     * @param segment The log segment to schedule for deletion
     */
    private void deleteSegment(LogSegment segment) {
        info(String.format("Scheduling log segment %d for log %s for deletion.", segment.baseOffset, name));
        synchronized (lock) {
            segments.remove(segment.baseOffset);
            asyncDeleteSegment(segment);
        }
    }

    /**
     * Perform an asynchronous delete on the given file if it exists (otherwise do nothing)
     */
    private void asyncDeleteSegment(LogSegment segment) {
        segment.changeFileSuffixes("", Log.DeletedFileSuffix);
        Action deleteSeg = () -> {
            info(String.format("Deleting segment %d from log %s.", segment.baseOffset, name));
            segment.delete();
        };
        scheduler.schedule("delete-file", deleteSeg, config.fileDeleteDelayMs);
    }

    /**
     * Swap a new segment in place and delete one or more existing segments in a crash-safe manner. The old segments will
     * be asynchronously deleted.
     *
     * @param newSegment  The new log segment to add to the log
     * @param oldSegments The old log segments to delete from the log
     */
    void replaceSegments(LogSegment newSegment, List<LogSegment> oldSegments) {
        synchronized (lock) {
            // need to do this in two phases to be crash safe AND do the delete asynchronously;
            // if we crash in the middle of this we complete the swap in loadSegments()
            newSegment.changeFileSuffixes(Log.CleanedFileSuffix, Log.SwapFileSuffix);
            addSegment(newSegment);

            // delete the old files;
            for (LogSegment seg : oldSegments) {
                // remove the index entry;
                if (seg.baseOffset != newSegment.baseOffset)
                    segments.remove(seg.baseOffset);
                // delete segment;
                asyncDeleteSegment(seg);
            }
            // okay we are safe now, remove the swap suffix;
            newSegment.changeFileSuffixes(Log.SwapFileSuffix, "");
        }
    }


    /**
     * Add the given segment to the segments in this log. If this segment replaces an existing segment, delete it.
     *
     * @param segment The segment to add
     */
    public LogSegment addSegment(LogSegment segment) {
        return this.segments.put(segment.baseOffset, segment);
    }


    /**
     * Make log segment file name from offset bytes. All this does is pad out the offset number with zeros
     * so that ls sorts the files numerically.
     *
     * @param offset The offset to use in the file name
     * @return The filename
     */
    public static String filenamePrefixFromOffset(Long offset) {
        NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(20);
        nf.setMaximumFractionDigits(0);
        nf.setGroupingUsed(false);
        return nf.format(offset);
    }


    /**
     * Construct a log file name in the given dir with the given base offset
     *
     * @param dir    The directory in which the log will reside
     * @param offset The base offset of the log file
     */
    public static File logFilename(File dir, Long offset) {
        return new File(dir, filenamePrefixFromOffset(offset) + LogFileSuffix);
    }

    /**
     * Construct an index file name in the given dir using the given base offset
     *
     * @param dir    The directory in which the log will reside
     * @param offset The base offset of the log file
     */
    public static File indexFilename(File dir, Long offset) {
        return new File(dir, filenamePrefixFromOffset(offset) + IndexFileSuffix);
    }

    /**
     * Parse the topic and partition out of the directory name of a log
     */
    public static TopicAndPartition parseTopicPartitionName(String name) {
        Integer index = name.lastIndexOf('-');
        return new TopicAndPartition(name.substring(0, index), Integer.parseInt(name.substring(index + 1)));
    }


    /**
     * a log file
     */
    public static final String LogFileSuffix = ".log";

    /**
     * an index file
     */
    public static final String IndexFileSuffix = ".index";

    /**
     * a file that is scheduled to be deleted
     */
    public static final String DeletedFileSuffix = ".deleted";

    /**
     * A temporary file that is being used for log cleaning
     */
    public static final String CleanedFileSuffix = ".cleaned";

    /**
     * A temporary file used when swapping files into the log
     */
    public static final String SwapFileSuffix = ".swap";

/** Clean shutdown file that indicates the broker was cleanly shutdown in 0.8. This is required to maintain backwards compatibility
 * with 0.8 and avoid unnecessary log recovery when upgrading from 0.8 to 0.8.1 */
    /**
     * Get TODO rid of CleanShutdownFile in 0.8.2
     */
    public static final String CleanShutdownFile = ".kafka_cleanshutdown";

    private void newGuages() {
        newGauge("NumLogSegments",
                new Gauge<Integer>() {
                    @Override
                    public Integer value() {
                        return numberOfSegments();
                    }
                },
                tags);
        newGauge("LogStartOffset",
                new Gauge<Long>() {
                    @Override
                    public Long value() {
                        return logStartOffset();
                    }
                },
                tags);
        newGauge("LogEndOffset",
                new Gauge<Long>() {
                    @Override
                    public Long value() {
                        return logEndOffset();
                    }
                },
                tags);
        newGauge("Size",
                new Gauge<Long>() {
                    @Override
                    public Long value() {
                        return size();
                    }
                },
                tags);
    }

}

