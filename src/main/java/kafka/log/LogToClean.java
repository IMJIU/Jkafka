package kafka.log;

import java.util.Comparator;
import java.util.stream.LongStream;

/**
 * Helper class for a log, its topic/partition, and the last clean position
 */
public class LogToClean implements Comparator<LogToClean> {
    public TopicAndPartition topicPartition;
    public Log log;
    public Long firstDirtyOffset;
    public Long cleanBytes = log.logSegments(-1L, firstDirtyOffset - 1).stream().mapToLong(s->s.size()).sum();
    val dirtyBytes = log.logSegments(firstDirtyOffset, Math.max(firstDirtyOffset, log.activeSegment().baseOffset)).map(_.size).sum;
    val cleanableRatio = dirtyBytes / totalBytes.toDouble;
    public void totalBytes = cleanBytes + dirtyBytes;
    overridepublic Integer

    void compare(LogToClean that)math.

    signum(this.cleanableRatio-that.cleanableRatio).toInt;
}
}
        }
