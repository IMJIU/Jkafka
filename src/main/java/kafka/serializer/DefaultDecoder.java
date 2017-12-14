package kafka.serializer;

import kafka.utils.VerifiableProperties;

/**
 * The default implementation does nothing, just returns the same byte array it takes in.
 */
public class DefaultDecoder implements Decoder<byte[]> {
    VerifiableProperties props = null;

    public DefaultDecoder(VerifiableProperties props) {
        this.props = props;
    }

    public byte[] fromBytes(byte[] bytes) {
        return bytes;
    }
}