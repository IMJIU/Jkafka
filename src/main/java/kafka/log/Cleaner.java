package kafka.log;/**
 * Created by zhoulf on 2017/4/11.
 */

import kafka.common.LogCleaningAbortedException;
import kafka.func.ActionWithP;
import kafka.utils.Logging;
import kafka.utils.Throttler;
import kafka.utils.Time;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;

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
    public ActionWithP<TopicAndPartition> checkDone;


    /**
     * @param id           An identifier used for logging
     * @param offsetMap    The map used for deduplication
     * @param ioBufferSize The size of the buffers to use. Memory usage will be 2x this number as there is a read and write buffer.
     * @param throttler    The throttler instance to use for limiting I/O rate.
     * @param time         The time instance
     */
    public Cleaner(Integer id, OffsetMap offsetMap, Integer ioBufferSize, Integer maxIoBufferSize, Double dupBufferLoadFactor, Throttler throttler, Time time, ActionWithP<TopicAndPartition> checkDone) {
        this.id = id;
        this.offsetMap = offsetMap;
        this.ioBufferSize = ioBufferSize;
        this.maxIoBufferSize = maxIoBufferSize;
        this.dupBufferLoadFactor = dupBufferLoadFactor;
        this.throttler = throttler;
        this.time = time;
        this.checkDone = checkDone;
        this.logIdent = "Cleaner " + id + ": ";
    }

    @Override
    public String loggerName() {
        return LogCleaner.class.getName();
    }


    /* cleaning stats - one instance for the current (or next) cleaning cycle and one for the last completed cycle */
    val statsUnderlying = (new CleanerStats(time), new CleanerStats(time));
    public stats =statsUnderlying._1;

    /* buffer used for read i/o */
    private ByteBuffer readBuffer = ByteBuffer.allocate(ioBufferSize);

    /* buffer used for write i/o */
    private ByteBuffer writeBuffer = ByteBuffer.allocate(ioBufferSize);

    /**
     * Clean the given log
     *
     * @param cleanable The log to be cleaned
     * @return The first offset not cleaned
     */
    public Long clean(LogToClean cleanable) {
        stats.clear();
        info(String.format("Beginning cleaning of log %s.", cleanable.log.name))
        val log = cleanable.log;

        // build the offset map;
        info(String.format("Building offset map for %s...", cleanable.log.name))
        val upperBoundOffset = log.activeSegment.baseOffset;
        val endOffset = buildOffsetMap(log, cleanable.firstDirtyOffset, upperBoundOffset, offsetMap) + 1;
        stats.indexDone();

        // figure out the timestamp below which it is safe to remove delete tombstones;
        // this position is defined to be a configurable time beneath the last modified time of the last clean segment;
        val deleteHorizonMs =
                log.logSegments(0, cleanable.firstDirtyOffset).lastOption match {
            case None =>0L;
            case Some(seg) =>seg.lastModified - log.config.deleteRetentionMs;
        }

        // group the segments and clean the groups;
        info(String.format("Cleaning log %s (discarding tombstones prior to %s)...", log.name, new Date(deleteHorizonMs)))
        for (group< -groupSegmentsBySize(log.logSegments(0, endOffset), log.config.segmentSize, log.config.maxIndexSize))
            cleanSegments(log, group, offsetMap, deleteHorizonMs);

        // record buffer utilization;
        stats.bufferUtilization = offsetMap.utilization;

        stats.allDone();

        endOffset;
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
            info(String.format("Swapping in cleaned segment %d for segment(s) %s in log %s.", cleaned.baseOffset, segments.map(_.baseOffset).mkString(","), log.name))
            log.replaceSegments(cleaned, segments);
        } catch (LogCleaningAbortedException e) {
            cleaned.delete();
            throw e;
        }
    }

    /**
     * Clean the given source log segment into the destination segment using the key=>offset mapping
     * provided
     *
     * @param source The dirty log segment
     * @param dest The cleaned log segment
     * @param map The key=>offset mapping
     * @param retainDeletes Should delete tombstones be retained while cleaning this segment
     *
     * Implement TODO proper compression support
     */
    private<log>

    public void cleanInto(TopicAndPartition topicAndPartition, LogSegment source,
                          LogSegment dest, OffsetMap map, Boolean retainDeletes) {
        var position = 0;
        while (position < source.log.sizeInBytes) {
            checkDone(topicAndPartition);
            // read a chunk of messages and copy any that are to be retained to the write buffer to be written out;
            readBuffer.clear();
            writeBuffer.clear();
            val messages = new ByteBufferMessageSet(source.log.readInto(readBuffer, position));
            throttler.maybeThrottle(messages.sizeInBytes);
            // check each message to see if it is to be retained;
            var messagesRead = 0;
            for (entry< -messages) {
                messagesRead += 1;
                val size = MessageSet.entrySize(entry.message);
                position += size;
                stats.readMessage(size);
                val key = entry.message.key;
                require(key != null, String.format("Found null key in log segment %s which is marked as dedupe.", source.log.file.getAbsolutePath))
                val foundOffset = map.get(key);
        /* two cases in which we can get rid of a message:
         *   1) if there exists a message with the same key but higher offset
         *   2) if the message is a delete "tombstone" marker and enough time has passed
         */
                val redundant = foundOffset >= 0 && entry.offset < foundOffset;
                val obsoleteDelete = !retainDeletes && entry.message.isNull;
                if (!redundant && !obsoleteDelete) {
                    ByteBufferMessageSet.writeMessage(writeBuffer, entry.message, entry.offset);
                    stats.recopyMessage(size);
                }
            }
            // if any messages are to be retained, write them out;
            if (writeBuffer.position > 0) {
                writeBuffer.flip();
                val retained = new ByteBufferMessageSet(writeBuffer);
                dest.append(retained.head.offset, retained);
                throttler.maybeThrottle(writeBuffer.limit);
            }

            // if we read bytes but didn't get even one complete message, our I/O buffer is too small, grow it and try again;
            if (readBuffer.limit > 0 && messagesRead == 0)
                growBuffers();
        }
        restoreBuffers();
    }

    /**
     * Double the I/O buffer capacity
     */
    public void growBuffers() {
        if (readBuffer.capacity >= maxIoBufferSize || writeBuffer.capacity >= maxIoBufferSize)
            throw new IllegalStateException(String.format("This log contains a message larger than maximum allowable size of %s.", maxIoBufferSize))
        val newSize = math.min(this.readBuffer.capacity * 2, maxIoBufferSize);
        info("Growing cleaner I/O buffers from " + readBuffer.capacity + "bytes to " + newSize + " bytes.");
        this.readBuffer = ByteBuffer.allocate(newSize);
        this.writeBuffer = ByteBuffer.allocate(newSize);
    }

    /**
     * Restore the I/O buffer capacity to its original size
     */
    public void restoreBuffers() {
        if (this.readBuffer.capacity > this.ioBufferSize)
            this.readBuffer = ByteBuffer.allocate(this.ioBufferSize);
        if (this.writeBuffer.capacity > this.ioBufferSize)
            this.writeBuffer = ByteBuffer.allocate(this.ioBufferSize);
    }

    /**
     * Group the segments in a log into groups totaling less than a given size. the size is enforced separately for the log data and the index data.
     * We collect a group of such segments together into a single
     * destination segment. This prevents segment sizes from shrinking too much.
     *
     * @param segments The log segments to group
     * @param maxSize the maximum size in bytes for the total of all log data in a group
     * @param maxIndexSize the maximum size in bytes for the total of all index data in a group
     *
     * @return A list of grouped segments
     */
    private<log>

    public void groupSegmentsBySize(Iterable segments<LogSegment>, Int maxSize, Int maxIndexSize):List<Seq[LogSegment]>=

    {
        var grouped = List < List[LogSegment] > ();
        var segs = segments.toList;
        while (!segs.isEmpty) {
            var group = List(segs.head);
            var logSize = segs.head.size;
            var indexSize = segs.head.index.sizeInBytes;
            segs = segs.tail;
            while ( !segs.isEmpty &&;
            logSize + segs.head.size < maxSize &&;
            indexSize + segs.head.index.sizeInBytes < maxIndexSize){
                group = segs.head::group;
                logSize += segs.head.size;
                indexSize += segs.head.index.sizeInBytes;
                segs = segs.tail;
            }
            grouped:: = group.reverse;
        }
        grouped.reverse;
    }

    /**
     * Build a map of key_hash => offset for the keys in the dirty portion of the log to use in cleaning.
     * @param log The log to use
     * @param start The offset at which dirty messages begin
     * @param end The ending offset for the map that is being built
     * @param map The map in which to store the mappings
     *
     * @return The final offset the map covers
     */
    private<log>public Long

    void buildOffsetMap(Log log, Long start, Long end, OffsetMap map) {
        map.clear();
        val dirty = log.logSegments(start, end).toSeq;
        info(String.format("Building offset map for log %s for %d segments in offset range [%d, %d).", log.name, dirty.size, start, end))

        // Add all the dirty segments. We must take at least map.slots * load_factor,
        // but we may be able to fit more (if there is lots of duplication in the dirty section of the log)
        var offset = dirty.head.baseOffset;
        require(offset == start, String.format("Last clean offset is %d but segment base offset is %d for log %s.", start, offset, log.name))
        val minStopOffset = (start + map.slots * this.dupBufferLoadFactor).toLong;
        for (segment< -dirty) {
            checkDone(log.topicAndPartition);
            if (segment.baseOffset <= minStopOffset || map.utilization < this.dupBufferLoadFactor)
                offset = buildOffsetMapForSegment(log.topicAndPartition, segment, map);
        }
        info(String.format("Offset map for log %s complete.", log.name))
        offset;
    }

    /**
     * Add the messages in the given segment to the offset map
     *
     * @param segment The segment to index
     * @param map The map in which to store the key=>offset mapping
     *
     * @return The final offset covered by the map
     */
    private public Long

    void buildOffsetMapForSegment(TopicAndPartition topicAndPartition, LogSegment segment, OffsetMap map) {
        var position = 0;
        var offset = segment.baseOffset;
        while (position < segment.log.sizeInBytes) {
            checkDone(topicAndPartition);
            readBuffer.clear();
            val messages = new ByteBufferMessageSet(segment.log.readInto(readBuffer, position));
            throttler.maybeThrottle(messages.sizeInBytes);
            val startPosition = position;
            for (entry< -messages) {
                val message = entry.message;
                require(message.hasKey);
                val size = MessageSet.entrySize(message);
                position += size;
                map.put(message.key, entry.offset);
                offset = entry.offset;
                stats.indexMessage(size);
            }
            // if we didn't read even one complete message, our read buffer may be too small;
            if (position == startPosition)
                growBuffers();
        }
        restoreBuffers();
        offset;
    }
}
}
