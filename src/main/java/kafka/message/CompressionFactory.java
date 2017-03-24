package kafka.message;/**
 * Created by zhoulf on 2017/3/24.
 */

import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author
 * @create 2017-03-24 11:20
 **/
public class CompressionFactory {


    public static OutputStream apply(CompressionCodec compressionCodec, OutputStream stream){
        compressionCodec match {
            case DefaultCompressionCodec => new GZIPOutputStream(stream)
            case GZIPCompressionCodec => new GZIPOutputStream(stream)
            case SnappyCompressionCodec =>
        import org.xerial.snappy.SnappyOutputStream
                new SnappyOutputStream(stream)
            case LZ4CompressionCodec =>
                new KafkaLZ4BlockOutputStream(stream)
            case _ =>
                throw new kafka.common.UnknownCodecException("Unknown Codec: " + compressionCodec)
        }
    }
    public static InputStream apply(CompressionCodec compressionCodec, InputStream stream){
        compressionCodec match {
            case DefaultCompressionCodec => new GZIPInputStream(stream)
            case GZIPCompressionCodec => new GZIPInputStream(stream)
            case SnappyCompressionCodec =>
        import org.xerial.snappy.SnappyInputStream
                new SnappyInputStream(stream)
            case LZ4CompressionCodec =>
                new KafkaLZ4BlockInputStream(stream)
            case _ =>
                throw new kafka.common.UnknownCodecException("Unknown Codec: " + compressionCodec)
        }
    }
}
