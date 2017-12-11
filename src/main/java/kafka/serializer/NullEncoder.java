package kafka.serializer;

import kafka.utils.VerifiableProperties;


public class NullEncoder<T> implements Encoder<T> {
    VerifiableProperties props = null;

    public NullEncoder(VerifiableProperties props) {
        this.props = props;
    }

    @Override
    public byte[] toBytes(T value) {
        return null;
    }
}



