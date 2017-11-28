package kafka.utils;

import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

import java.io.UnsupportedEncodingException;

/**
 * @author zhoulf
 * @create 2017-11-27 18:55
 **/
public class ZKStringSerializer implements ZkSerializer {

    //        @throws(classOf<ZkMarshallingError>)
    public byte[] serialize(Object data) {
        try {
            return ((String) data).getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new ZkMarshallingError();
        }
    }

    //        @throws(classOf<ZkMarshallingError>)
    public Object deserialize(byte[] bytes) {
        if (bytes == null)
            return null;
        else
            try {
                return new String(bytes, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new ZkMarshallingError();
            }
    }
}