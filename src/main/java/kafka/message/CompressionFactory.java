package kafka.message;/**
 * Created by zhoulf on 2017/3/24.
 */

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * @author
 * @create 2017-03-24 11:20
 **/
public class CompressionFactory {
    public static OutputStream apply(CompressionCodec compressionCodec, OutputStream stream) {
        try {
            switch (compressionCodec) {
                case DefaultCompressionCodec:
                    return new GZIPOutputStream(stream);
                case GZIPCompressionCodec:
                    return new GZIPOutputStream(stream);
                case SnappyCompressionCodec:
                    return new SnappyOutputStream(stream);
                case LZ4CompressionCodec:
                    return new KafkaLZ4BlockOutputStream(stream);
                default:
                    throw new kafka.common.UnknownCodecException("Unknown Codec: " + compressionCodec);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static InputStream apply(CompressionCodec compressionCodec, InputStream stream) {
        try {
            switch (compressionCodec) {
                case DefaultCompressionCodec:
                    return new GZIPInputStream(stream);
                case GZIPCompressionCodec:
                    return new GZIPInputStream(stream);
                case SnappyCompressionCodec:
                    return new SnappyInputStream(stream);
                case LZ4CompressionCodec:
                    return new KafkaLZ4BlockInputStream(stream);
                default:
                    throw new kafka.common.UnknownCodecException("Unknown Codec: " + compressionCodec);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
