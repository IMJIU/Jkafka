package kafka.message;/**
 * Created by zhoulf on 2017/3/24.
 */

import kafka.utils.Logging;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

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
    private static final Logging logger = Logging.getLogger(CompressionFactory.class.getName());
    public static OutputStream outputStream(CompressionCodec compressionCodec, OutputStream stream) {
        try {
            switch (compressionCodec) {
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
            logger.error(e.getMessage(),e);
        }
        return null;
    }

    public static InputStream inputStream(CompressionCodec compressionCodec, InputStream stream) {
        try {
            switch (compressionCodec) {
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
            logger.error(e.getMessage(),e);
        }
        return null;
    }
}
