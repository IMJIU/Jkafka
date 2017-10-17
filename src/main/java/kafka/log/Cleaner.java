package kafka.log;/**
 * Created by zhoulf on 2017/4/11.
 */

import com.google.common.collect.Lists;
import kafka.common.LogCleaningAbortedException;
import kafka.func.ActionP;
import kafka.func.Tuple;
import kafka.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import kafka.message.MessageSet;
import kafka.utils.Logging;
import kafka.utils.Prediction;
import kafka.utils.Throttler;
import kafka.utils.Time;
import org.apache.commons.collections.CollectionUtils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This class holds the actual logic for cleaning a log
 **/
public class Cleaner extends Logging {
    public Integer id;
    public OffsetMap offsetMap;
    public Integer ioBufferSize;
    public Integer maxIoBufferSize;
    public Double dupBufferLoadFactor;
    public Throttler throttler;
    public Time time;
    public ActionP<TopicAndPartition> checkDone;


    /* cleaning stats - one instance for the current (or next) cleaning cycle and one for the last completed cycle */
    Tuple<CleanerStats, CleanerStats> statsUnderlying;
    public CleanerStats stats;

    /* buffer used for read i/o */
    private ByteBuffer readBuffer;

    /* buffer used for write i/o */
    private ByteBuffer writeBuffer;

    /**
     * @param id           An identifier used for logging
     * @param offsetMap    The map used for deduplication
     * @param ioBufferSize The size of the buffers to use. Memory usage will be 2x this number as there is a read and write buffer.
     * @param throttler    The throttler instance to use for limiting I/O rate.
     * @param time         The time instance
     */
    public Cleaner(Integer id, OffsetMap offsetMap, Integer ioBufferSize, Integer maxIoBufferSize, Double dupBufferLoadFactor, Throttler throttler, Time time, ActionP<TopicAndPartition> checkDone) {
        this.id = id;
        this.offsetMap = offsetMap;
        this.ioBufferSize = ioBufferSize;
        this.maxIoBufferSize = maxIoBufferSize;
        this.dupBufferLoadFactor = dupBufferLoadFactor;
        this.throttler = throttler;
        this.time = time;
        this.checkDone = checkDone;
        this.logIdent = "Cleaner " + id + ": ";
        loggerName(LogCleaner.class.getName());
        statsUnderlying = Tuple.of(new CleanerStats(time), new CleanerStats(time));
        stats = statsUnderlying.v1;
        readBuffer = ByteBuffer.allocate(ioBufferSize);
        writeBuffer = ByteBuffer.allocate(ioBufferSize);
    }

    /**
     * Clean the given log
     *
     * @param cleanable The log to be cleaned
     * @return The first offset not cleaned
     */
    public Long clean(LogToClean cleanable) throws IOException, InterruptedException {
        stats.clear();
        info(String.format("Beginning cleaning of log %s.", cleanable.log.name));
        Log log = cleanable.log;

        // build the offset map;
        info(String.format("Building offset map for %s...", cleanable.log.name));
        Long upperBoundOffset = log.activeSegment().baseOffset;
        Long endOffset = buildOffsetMap(log, cleanable.firstDirtyOffset, upperBoundOffset, offsetMap) + 1;
        stats.indexDone();

        // figure out the timestamp below which it is safe to remove delete tombstones;
        // this position is defined to be a configurable time beneath the last modified time of the last clean segment;
        List<LogSegment> list = Lists.newArrayList(log.logSegments(0L, cleanable.firstDirtyOffset));
        Long deleteHorizonMs;
        if (CollectionUtils.isEmpty(list)) {
            deleteHorizonMs = 0L;
        } else {
            deleteHorizonMs = list.get(list.size() - 1).lastModified() - log.config.deleteRetentionMs;
        }

        // group the segments and clean the groups;
        info(String.format("Cleaning log %s (discarding tombstones prior to %s)...", log.name, new Date(deleteHorizonMs)));
        for (List<LogSegment> group : groupSegmentsBySize(log.logSegments(0L, endOffset), log.config.segmentSize, log.config.maxIndexSize))
            cleanSegments(log, group, offsetMap, deleteHorizonMs);

        // record buffer utilization;
        stats.bufferUtilization = offsetMap.utilization();

        stats.allDone();

        return endOffset;
    }

    /**
     * Clean a group of segments into a single replacement segment
     *
     * @param log             The log being cleaned
     * @param segments        The group of segments being cleaned
     * @param map             The offset map to use for cleaning segments
     * @param deleteHorizonMs The time to retain delete tombstones
     */
    public void cleanSegments(Log log,
                              List<LogSegment> segments,
                              OffsetMap map,
                              Long deleteHorizonMs) throws IOException {
        // create a new segment with the suffix .cleaned appended to both the log and index name;
        File logFile = new File(segments.get(0).log.file.getPath() + Log.CleanedFileSuffix);
        logFile.delete();
        File indexFile = new File(segments.get(0).index.file.getPath() + Log.CleanedFileSuffix);
        indexFile.delete();
        FileMessageSet messages = new FileMessageSet(logFile);
        OffsetIndex index = new OffsetIndex(indexFile, segments.get(0).baseOffset, segments.get(0).index.maxIndexSize);
        LogSegment cleaned = new LogSegment(messages, index, segments.get(0).baseOffset, segments.get(0).indexIntervalBytes, log.config.randomSegmentJitter, time);

        try {
            // clean segments into the new destination segment;
            for (LogSegment old : segments) {
                boolean retainDeletes = old.lastModified() > deleteHorizonMs;
                info(String.format("Cleaning segment %s in log %s (last modified %s) into %s, %s deletes.", old.baseOffset, log.name, new Date(old.lastModified()), cleaned.baseOffset, retainDeletes ? "retaining" : "discarding"));
                cleanInto(log.topicAndPartition, old, cleaned, map, retainDeletes);
            }

            // trim excess index;
            index.trimToValidSize();

            // flush new segment to disk before swap;
            cleaned.flush();

            // update the modification date to retain the last modified date of the original files;
            Long modified = segments.get(segments.size() - 1).lastModified();
            cleaned.setLastModified(modified);

            // swap in new segment;
            info(String.format("Swapping in cleaned segment %d for segment(s) %s in log %s.", cleaned.baseOffset, segments.stream().map(s -> s.baseOffset).collect(Collectors.toList()), log.name));
            log.replaceSegments(cleaned, segments);
        } catch (LogCleaningAbortedException e) {
            cleaned.delete();
            throw e;
        } catch (InterruptedException e) {
            error(e.getMessage(), e);
        }
    }

    /**
     * Clean the given source log segment into the destination segment using the key=>offset mapping
     * provided
     *
     * @param source        The dirty log segment
     * @param dest          The cleaned log segment
     * @param map           The key=>offset mapping
     * @param retainDeletes Should delete tombstones be retained while cleaning this segment
     *                      <p>
     *                      Implement TODO proper compression support
     */
    void cleanInto(TopicAndPartition topicAndPartition, LogSegment source,
                   LogSegment dest, OffsetMap map, Boolean retainDeletes) throws IOException, InterruptedException {
        Integer position = 0;
        while (position < source.log.sizeInBytes()) {
            checkDone.invoke(topicAndPartition);
            // read a chunk of messages and copy any that are to be retained to the write buffer to be written out;
            readBuffer.clear();
            writeBuffer.clear();
            ByteBufferMessageSet messages = new ByteBufferMessageSet(source.log.readInto(readBuffer, position));
            throttler.maybeThrottle((double) messages.sizeInBytes());
            // check each message to see if it is to be retained;
            Integer messagesRead = 0;
            for (MessageAndOffset entry : messages) {
                messagesRead += 1;
                Integer size = MessageSet.entrySize(entry.message);
                position += size;
                stats.readMessage(size);
                ByteBuffer key = entry.message.key();
                Prediction.require(key != null, String.format("Found null key in log segment %s which is marked as dedupe.", source.log.file.getAbsolutePath()));
                Long foundOffset = map.get(key);
                /* two cases in which we can get rid of a message:
                 *   1) if there exists a message with the same key but higher offset
                 *   2) if the message is a delete "tombstone" marker and enough time has passed
                 */
                boolean redundant = foundOffset >= 0 && entry.offset < foundOffset;
                boolean obsoleteDelete = !retainDeletes && entry.message.isNull();
                if (!redundant && !obsoleteDelete) {
                    ByteBufferMessageSet.writeMessage(writeBuffer, entry.message, entry.offset);
                    stats.recopyMessage(size);
                }
            }
            // if any messages are to be retained, write them out;
            if (writeBuffer.position() > 0) {
                writeBuffer.flip();
                ByteBufferMessageSet retained = new ByteBufferMessageSet(writeBuffer);
                dest.append(retained.head().offset, retained);
                throttler.maybeThrottle((double) writeBuffer.limit());
            }

            // if we read bytes but didn't get even one complete message, our I/O buffer is too small, grow it and try again;
            if (readBuffer.limit() > 0 && messagesRead == 0)
                growBuffers();
        }
        restoreBuffers();
    }

    /**
     * Double the I/O buffer capacity
     */
    public void growBuffers() {
        if (readBuffer.capacity() >= maxIoBufferSize || writeBuffer.capacity() >= maxIoBufferSize)
            throw new IllegalStateException(String.format("This log contains a message larger than maximum allowable size of %s.", maxIoBufferSize));
        Integer newSize = Math.min(this.readBuffer.capacity() * 2, maxIoBufferSize);
        info("Growing cleaner I/O buffers from " + readBuffer.capacity() + "bytes to " + newSize + " bytes.");
        this.readBuffer = ByteBuffer.allocate(newSize);
        this.writeBuffer = ByteBuffer.allocate(newSize);
    }

    /**
     * Restore the I/O buffer capacity to its original size
     */
    public void restoreBuffers() {
        if (this.readBuffer.capacity() > this.ioBufferSize)
            this.readBuffer = ByteBuffer.allocate(this.ioBufferSize);
        if (this.writeBuffer.capacity() > this.ioBufferSize)
            this.writeBuffer = ByteBuffer.allocate(this.ioBufferSize);
    }

    /**
     * Group the segments in a log into groups totaling less than a given size. the size is enforced separately for the log data and the index data.
     * We collect a group of such segments together into a single
     * destination segment. This prevents segment sizes from shrinking too much.
     *
     * @param segments     The log segments to group
     * @param maxSize      the maximum size in bytes for the total of all log data in a group
     * @param maxIndexSize the maximum size in bytes for the total of all index data in a group
     * @return A list of grouped segments
     */
    List<List<LogSegment>> groupSegmentsBySize(Collection<LogSegment> segments, Integer maxSize, Integer maxIndexSize) {
        List<LogSegment> segs = Lists.newArrayList(segments);
        List<List<LogSegment>> grouped = Lists.newArrayList();
        while (!segs.isEmpty()) {
            List<LogSegment> group = Lists.newArrayList(segs.get(0));
            Long logSize = segs.get(0).size();
            Integer indexSize = segs.get(0).index.sizeInBytes();
            segs = segs.subList(1, segs.size());
            while (!segs.isEmpty() &&
                    logSize + segs.get(0).size() < maxSize &&
                    indexSize + segs.get(0).index.sizeInBytes() < maxIndexSize) {
//                group = segs.get(0)::group;
                logSize += segs.get(0).size();
                indexSize += segs.get(0).index.sizeInBytes();
                segs = segs.subList(1, segs.size());
            }
//            grouped:: = group.reverse;
        }
//        grouped.reverse;
        return null;
    }

    /**
     * Build a map of key_hash => offset for the keys in the dirty portion of the log to use in cleaning.
     *
     * @param log   The log to use
     * @param start The offset at which dirty messages begin
     * @param end   The ending offset for the map that is being built
     * @param map   The map in which to store the mappings
     * @return The final offset the map covers
     */
    Long buildOffsetMap(Log log, Long start, Long end, OffsetMap map) throws IOException, InterruptedException {
        map.clear();
        List<LogSegment> dirty = Lists.newArrayList(log.logSegments(start, end));
        info(String.format("Building offset map for log %s for %d segments in offset range [%d, %d).", log.name, dirty.size(), start, end));

        // Add all the dirty segments. We must take at least map.slots * load_factor,
        // but we may be able to fit more (if there is lots of duplication in the dirty section of the log)
        Long offset = dirty.get(0).baseOffset;
        Prediction.require(offset == start, String.format("Last clean offset is %d but segment base offset is %d for log %s.", start, offset, log.name));
        Long minStopOffset = (long) (start + map.slots() * this.dupBufferLoadFactor);
        for (LogSegment segment : dirty) {
            checkDone.invoke(log.topicAndPartition);
            if (segment.baseOffset <= minStopOffset || map.utilization() < this.dupBufferLoadFactor)
                offset = buildOffsetMapForSegment(log.topicAndPartition, segment, map);
        }
        info(String.format("Offset map for log %s complete.", log.name));
        return offset;
    }

    /**
     * Add the messages in the given segment to the offset map
     *
     * @param segment The segment to index
     * @param map     The map in which to store the key=>offset mapping
     * @return The final offset covered by the map
     */
    private Long buildOffsetMapForSegment(TopicAndPartition topicAndPartition, LogSegment segment, OffsetMap map) throws IOException, InterruptedException {
        Integer position = 0;
        Long offset = segment.baseOffset;
        while (position < segment.log.sizeInBytes()) {
            checkDone.invoke(topicAndPartition);
            readBuffer.clear();
            ByteBufferMessageSet messages = new ByteBufferMessageSet(segment.log.readInto(readBuffer, position));
            throttler.maybeThrottle(new Double(messages.sizeInBytes()));
            Integer startPosition = position;
            for (MessageAndOffset entry : messages) {
                Message message = entry.message;
                Prediction.require(message.hasKey());
                Integer size = MessageSet.entrySize(message);
                position += size;
                map.put(message.key(), entry.offset);
                offset = entry.offset;
                stats.indexMessage(size);
            }
            // if we didn't read even one complete message, our read buffer may be too small;
            if (position == startPosition)
                growBuffers();
        }
        restoreBuffers();
        return offset;
    }
}

class CleanerConfig {
    public Integer numThreads = 1;
    public Long dedupeBufferSize = 4 * 1024 * 1024L;
    public Double dedupeBufferLoadFactor = 0.9d;
    public Integer ioBufferSize = 1024 * 1024;
    public Integer maxMessageSize = 32 * 1024 * 1024;
    public Double maxIoBytesPerSecond = java.lang.Double.MAX_VALUE;
    public Long backOffMs = 15 * 1000L;
    public Boolean enableCleaner = true;
    public String hashAlgorithm = "MD5";

    /**
     * Configuration parameters for the log cleaner
     *
     * @param numThreads             The number of cleaner threads to run
     * @param dedupeBufferSize       The total memory used for log deduplication
     * @param dedupeBufferLoadFactor The maximum percent full for the deduplication buffer
     * @param maxMessageSize         The maximum size of a message that can appear in the log
     * @param maxIoBytesPerSecond    The maximum read and write I/O that all cleaner threads are allowed to do
     * @param backOffMs              The amount of time to wait before rechecking if no logs are eligible for cleaning
     * @param enableCleaner          Allows completely disabling the log cleaner
     * @param hashAlgorithm          The hash algorithm to use in key comparison.
     */
    public CleanerConfig(Integer numThreads, Long dedupeBufferSize, Double dedupeBufferLoadFactor, Integer ioBufferSize, Integer maxMessageSize, Double maxIoBytesPerSecond, Long backOffMs, Boolean enableCleaner, String hashAlgorithm) {
        this.numThreads = numThreads;
        this.dedupeBufferSize = dedupeBufferSize;
        this.dedupeBufferLoadFactor = dedupeBufferLoadFactor;
        this.ioBufferSize = ioBufferSize;
        this.maxMessageSize = maxMessageSize;
        this.maxIoBytesPerSecond = maxIoBytesPerSecond;
        this.backOffMs = backOffMs;
        this.enableCleaner = enableCleaner;
        this.hashAlgorithm = hashAlgorithm;
    }

    public CleanerConfig() {
    }

    public CleanerConfig(Boolean enableCleaner) {
        this.enableCleaner = enableCleaner;
    }
}

class CleanerStats {
    public Time time;

    /**
     * A simple struct for collecting stats about log cleaning
     */
    public CleanerStats(Time time) {
        this.time = time;
        clear();
        elapsedSecs = (endTime - startTime) / 1000.0;
        elapsedIndexSecs = (mapCompleteTime - startTime) / 1000.0;

    }

    public Long startTime, mapCompleteTime, endTime, bytesRead, bytesWritten, mapBytesRead, mapMessagesRead, messagesRead, messagesWritten = 0L;
    public Double bufferUtilization = 0.0d;

    public void readMessage(Integer size) {
        messagesRead += 1;
        bytesRead += size;
    }

    public void recopyMessage(Integer size) {
        messagesWritten += 1;
        bytesWritten += size;
    }

    public void indexMessage(Integer size) {
        mapMessagesRead += 1;
        mapBytesRead += size;
    }

    public void indexDone() {
        mapCompleteTime = time.milliseconds();
    }

    public void allDone() {
        endTime = time.milliseconds();
    }

    public Double elapsedSecs;

    public Double elapsedIndexSecs;

    public void clear() {
        startTime = time.milliseconds();
        mapCompleteTime = -1L;
        endTime = -1L;
        bytesRead = 0L;
        bytesWritten = 0L;
        mapBytesRead = 0L;
        mapMessagesRead = 0L;
        messagesRead = 0L;
        messagesWritten = 0L;
        bufferUtilization = 0.0d;
    }
}