package kafka.serializer;

import kafka.utils.VerifiableProperties;

import java.io.UnsupportedEncodingException;

/**
 * The string encoder translates strings into bytes. It uses UTF8 by default but takes
 * an optional property serializer.encoding to control this.
 */
public class StringDecoder implements Decoder<String> {
    VerifiableProperties props = null;

    public StringDecoder(VerifiableProperties props) {
        this.props = props;
        if (props == null)
            encoding = "UTF8";
        else
            encoding = props.getString("serializer.encoding", "UTF8");
    }

    public String encoding;


    public String fromBytes(byte[] bytes) {
        try {
            return new String(bytes, encoding);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
}