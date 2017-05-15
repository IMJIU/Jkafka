package kafka.common;/**
 * Created by zhoulf on 2017/5/15.
 */

/**
 * @author
 * @create 2017-05-15 18:11
 **/
public class OffsetAndMetadata {

case class OffsetAndMetadata(offset: Long,
    metadata: String = OffsetAndMetadata.NoMetadata,
    var timestamp: Long = -1L) {
        override def toString = "OffsetAndMetadata[%d,%s%s]"
                .format(offset,
        if (metadata != null && metadata.length > 0) metadata else "NO_METADATA",
        if (timestamp == -1) "" else "," + timestamp.toString)
    }

    object OffsetAndMetadata {
        val InvalidOffset: Long = -1L
        val NoMetadata: String = ""
        val InvalidTime: Long = -1L
    }

case class OffsetMetadataAndError(offset: Long,
    metadata: String = OffsetAndMetadata.NoMetadata,
    error: Short = ErrorMapping.NoError) {

        def this(offsetMetadata: OffsetAndMetadata, error: Short) =
        this(offsetMetadata.offset, offsetMetadata.metadata, error)

        def this(error: Short) =
        this(OffsetAndMetadata.InvalidOffset, OffsetAndMetadata.NoMetadata, error)

        def asTuple = (offset, metadata, error)

        override def toString = "OffsetMetadataAndError[%d,%s,%d]".format(offset, metadata, error)
    }

    object OffsetMetadataAndError {
        val NoOffset = OffsetMetadataAndError(OffsetAndMetadata.InvalidOffset, OffsetAndMetadata.NoMetadata, ErrorMapping.NoError)
        val OffsetsLoading = OffsetMetadataAndError(OffsetAndMetadata.InvalidOffset, OffsetAndMetadata.NoMetadata, ErrorMapping.OffsetsLoadInProgressCode)
        val NotOffsetManagerForGroup = OffsetMetadataAndError(OffsetAndMetadata.InvalidOffset, OffsetAndMetadata.NoMetadata, ErrorMapping.NotCoordinatorForConsumerCode)
        val UnknownTopicOrPartition = OffsetMetadataAndError(OffsetAndMetadata.InvalidOffset, OffsetAndMetadata.NoMetadata, ErrorMapping.UnknownTopicOrPartitionCode)
    }
}
