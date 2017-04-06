package kafka.message;/**
 * Created by zhoulf on 2017/3/22.
 */

/**
 * @author
 * @create 2017-03-22 20:27
 **/
public enum CompressionCodec {
    NoCompressionCodec(0, "none"),
    GZIPCompressionCodec(1, "gzip"),
    SnappyCompressionCodec(2, "snappy"),
    LZ4CompressionCodec(3, "none");
    public int codec;
    public String name;

    CompressionCodec(int c, String n) {
        this.codec = c;
        this.name = n;
    }

    public static CompressionCodec getCompressionCodec(Integer c) {
        if (c.equals(NoCompressionCodec.codec))
            return NoCompressionCodec;
        if (c.equals(GZIPCompressionCodec.codec))
            return GZIPCompressionCodec;
        if (c.equals(SnappyCompressionCodec.codec))
            return SnappyCompressionCodec;
        if (c.equals(LZ4CompressionCodec.codec))
            return LZ4CompressionCodec;
        throw new kafka.common.UnknownCodecException("%d is an unknown compression codec".format(c.toString()));
    }

    public static CompressionCodec getCompressionCodec(String name) {
        if (name.equals(NoCompressionCodec.name))
            return NoCompressionCodec;
        if (name.equals(GZIPCompressionCodec.name))
            return GZIPCompressionCodec;
        if (name.equals(SnappyCompressionCodec.name))
            return SnappyCompressionCodec;
        if (name.equals(LZ4CompressionCodec.name))
            return LZ4CompressionCodec;
        throw new kafka.common.UnknownCodecException("%d is an unknown compression codec".format(name));
    }
}



