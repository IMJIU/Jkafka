package kafka.log;

import java.util.Comparator;
import java.util.stream.LongStream;

/**
 * Helper class for a log, its topic/partition, and the last clean position
 */
public class LogToClean implements Comparable<LogToClean> {
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
