package kafka.common;/**
 * Created by zhoulf on 2017/5/15.
 */

/**
 * @author
 * @create 2017-05-15 11 18
 **/
public class OffsetAndMetadata {
    public static Long InvalidOffset = -1L;
    public static String NoMetadata = "";
    public static Long InvalidTime = -1L;
    public Long offset;
    public String metadata;
    public Long timestamp = -1L;

    public OffsetAndMetadata(java.lang.Long offset) {
        this(offset, null, null);
    }

    public OffsetAndMetadata(java.lang.Long offset, String metadata) {
        this(offset, metadata, null);
    }

    public OffsetAndMetadata(java.lang.Long offset, String metadata, java.lang.Long timestamp) {
        this.offset = offset;
        this.timestamp = timestamp;
        this.metadata = metadata;
        if (metadata == null) this.metadata = NoMetadata;
        if (timestamp == null) this.timestamp = -1L;
    }

    @Override
    public String toString() {
        return String.format("OffsetAndMetadata<%d,%s%s>", offset,
                metadata != null && metadata.length() > 0 ? metadata : "NO_METADATA",
                timestamp == -1 ? "" : "," + timestamp.toString());
    }
}
