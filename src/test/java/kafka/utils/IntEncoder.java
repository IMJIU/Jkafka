package kafka.utils;

import kafka.serializer.Encoder;

public class IntEncoder implements Encoder<Integer> {
    public VerifiableProperties props = null;

    public IntEncoder(VerifiableProperties props) {
        this.props = props;
    }

    public byte[] toBytes(Integer n) {
        return n.toString().getBytes();
    }

}