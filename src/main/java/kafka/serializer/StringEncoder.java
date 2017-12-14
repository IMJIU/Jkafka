package kafka.serializer;

import kafka.utils.VerifiableProperties;

import java.io.UnsupportedEncodingException;

/**
 * The string encoder takes an optional parameter serializer.encoding which controls
 * the character set used in encoding the string into bytes.
 */
public class StringEncoder implements Encoder<String> {
    VerifiableProperties props = null;

    public StringEncoder(VerifiableProperties props) {
        this.props = props;
        if (props == null)
            encoding = "UTF8";
        else
            encoding = props.getString("serializer.encoding", "UTF8");
    }

    String encoding;

    @Override
    public byte[] toBytes(String s) {
        if (s == null)
            return null;
        else
            try {
                return s.getBytes(encoding);
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
    }
}