package kafka.serializer;

/**
 * @author zhoulf
 * @create 2017-12-14 50 13
 **/

import kafka.utils.VerifiableProperties;

import java.io.UnsupportedEncodingException;

/**
 * A decoder is a method of turning byte arrays into objects.
 * An implementation is required to provide a constructor that
 * takes a VerifiableProperties instance.
 */
public interface Decoder<T> {
    T fromBytes(byte[] bytes);
}


