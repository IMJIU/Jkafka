package kafka.server;

/**
 * Created by Administrator on 2017/3/29.
 */
public class LogOffsetMetadata {
    public static final Long UnknownSegBaseOffset = -1L;
    public static final Integer UnknownFilePosition = -1;
    public static final LogOffsetMetadata UnknownOffsetMetadata = new LogOffsetMetadata(-1L, 0L, 0);

    public Long messageOffset;
    public Long segmentBaseOffset = LogOffsetMetadata.UnknownSegBaseOffset;
    public Integer relativePositionInSegment = LogOffsetMetadata.UnknownFilePosition;

    public LogOffsetMetadata(Long messageOffset, Long segmentBaseOffset, Integer positionInSegment) {
        this.messageOffset = messageOffset;
        this.segmentBaseOffset = segmentBaseOffset;
        this.relativePositionInSegment = positionInSegment;
    }

    public  static int compare(LogOffsetMetadata x,LogOffsetMetadata y) {
        return x.offsetDiff(y).intValue();
    }

    public Boolean precedes(LogOffsetMetadata that){
        return this.messageOffset < that.messageOffset;
    }

    //    class OffsetOrdering extends Ordering[LogOffsetMetadata] {
//        override def compare(x: LogOffsetMetadata , y: LogOffsetMetadata ): Int = {
//        return x.offsetDiff(y).toInt
//        }
//    }
    public Long offsetDiff(LogOffsetMetadata that) {
        return this.messageOffset - that.messageOffset;
    }

    public LogOffsetMetadata(Long messageOffset) {
        this(messageOffset, LogOffsetMetadata.UnknownSegBaseOffset, LogOffsetMetadata.UnknownFilePosition);
    }

    public LogOffsetMetadata(Long messageOffset, Long segmentBaseOffset) {
        this(messageOffset, segmentBaseOffset, LogOffsetMetadata.UnknownFilePosition);
    }


}
