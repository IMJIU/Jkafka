package kafka.log;

import kafka.message.CompressionCodec;

/**
 * Struct to hold various quantities we compute about each message set before appending to the log
 *
 */
public class LogAppendInfo {
    public Long firstOffset;
    public Long lastOffset;
    public CompressionCodec codec;
    public Integer shallowCount;
    public Integer validBytes;
    public Boolean offsetsMonotonic;

    /**
     *
     * @param firstOffset      The first offset in the message set
     * @param lastOffset       The last offset in the message set
     * @param shallowCount     The number of shallow messages
     * @param validBytes       The number of valid bytes
     * @param codec            The codec used in the message set
     * @param offsetsMonotonic Are the offsets in this message set monotonically increasing
     */
    public LogAppendInfo(Long firstOffset, Long lastOffset, CompressionCodec codec, Integer shallowCount, Integer validBytes, Boolean offsetsMonotonic) {
        this.firstOffset = firstOffset;
        this.lastOffset = lastOffset;
        this.codec = codec;
        this.shallowCount = shallowCount;
        this.validBytes = validBytes;
        this.offsetsMonotonic = offsetsMonotonic;
    }
}
