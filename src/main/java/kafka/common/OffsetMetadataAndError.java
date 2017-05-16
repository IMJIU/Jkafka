package kafka.common;/**
 * Created by zhoulf on 2017/5/16.
 */

import kafka.func.Tuple;
import kafka.func.Tuple3;

/**
 * @author
 * @create 2017-05-16 9:51
 **/
public class OffsetMetadataAndError {
    public static OffsetMetadataAndError NoOffset = new OffsetMetadataAndError(OffsetAndMetadata.InvalidOffset, OffsetAndMetadata.NoMetadata, ErrorMapping.NoError);
    public static OffsetMetadataAndError OffsetsLoading = new OffsetMetadataAndError(OffsetAndMetadata.InvalidOffset, OffsetAndMetadata.NoMetadata, ErrorMapping.OffsetsLoadInProgressCode);
    public static OffsetMetadataAndError NotOffsetManagerForGroup = new OffsetMetadataAndError(OffsetAndMetadata.InvalidOffset, OffsetAndMetadata.NoMetadata, ErrorMapping.NotCoordinatorForConsumerCode);
    public static OffsetMetadataAndError UnknownTopicOrPartition = new OffsetMetadataAndError(OffsetAndMetadata.InvalidOffset, OffsetAndMetadata.NoMetadata, ErrorMapping.UnknownTopicOrPartitionCode);

    public Long offset;
    public String metadata;
    public Short error;

    public OffsetMetadataAndError(Long offset, String metadata, Short error) {
        this.offset = offset;
        this.metadata = metadata;
        this.error = error;
        if (metadata == null) this.metadata = OffsetAndMetadata.NoMetadata;
        if (error == null) this.error = ErrorMapping.NoError;
    }


    public OffsetMetadataAndError(OffsetAndMetadata offsetMetadata, Short error) {
        this(offsetMetadata.offset, offsetMetadata.metadata, error);
    }

    public OffsetMetadataAndError(Short error) {
        this(OffsetAndMetadata.InvalidOffset, OffsetAndMetadata.NoMetadata, error);
    }

    public Tuple3<Long, String, Short> asTuple() {
        return Tuple3.of(offset, metadata, error);
    }

    @Override
    public String toString() {
        return String.format("OffsetMetadataAndError<%d,%s,%d>", offset, metadata, error);
    }
}


