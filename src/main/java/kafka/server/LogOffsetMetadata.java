package kafka.server;

import kafka.common.KafkaException;

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

    public LogOffsetMetadata(Long messageOffset, Long baseOffset, Integer positionInSegment) {
        this.messageOffset = messageOffset;
        this.segmentBaseOffset = baseOffset;
        this.relativePositionInSegment = positionInSegment;
    }

    public static int compare(LogOffsetMetadata x, LogOffsetMetadata y) {
        return x.offsetDiff(y).intValue();
    }

    public Boolean precedes(LogOffsetMetadata that) {
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

    public Boolean offsetOnOlderSegment(LogOffsetMetadata that) {
        if (messageOffsetOnly())
            throw new KafkaException(String.format("%s cannot compare its segment info with %s since it only has message offset info", this, that));

        return this.segmentBaseOffset < that.segmentBaseOffset;
    }

    // decide if the offset metadata only contains message offset info
    public boolean messageOffsetOnly() {
        return segmentBaseOffset == LogOffsetMetadata.UnknownSegBaseOffset && relativePositionInSegment == LogOffsetMetadata.UnknownFilePosition;
    }

    // compute the number of bytes between this offset to the given offset
    // if they are on the same segment and this offset precedes the given offset
    public int positionDiff(LogOffsetMetadata that) {
        if (!offsetOnSameSegment(that))
            throw new KafkaException(String.format("%s cannot compare its segment position with %s since they are not on the same segment", this, that));
        if (messageOffsetOnly())
            throw new KafkaException(String.format("%s cannot compare its segment position with %s since it only has message offset info", this, that));

        return this.relativePositionInSegment - that.relativePositionInSegment;
    }

    // check if this offset is on the same segment with the given offset
    public boolean offsetOnSameSegment(LogOffsetMetadata that) {
        if (messageOffsetOnly())
            throw new KafkaException(String.format("%s cannot compare its segment info with %s since it only has message offset info", this, that));

        return this.segmentBaseOffset == that.segmentBaseOffset;
    }

    @Override
    public String toString(){
        return messageOffset.toString() + " [" + segmentBaseOffset + " : " + relativePositionInSegment + "]";
    }
}
