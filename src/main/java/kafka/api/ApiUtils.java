package kafka.api;/**
 * Created by zhoulf on 2017/4/25.
 */

import kafka.common.KafkaException;
import kafka.func.Tuple;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

/**
 * Helper functions specific to parsing or serializing requests and responses
 */
public class ApiUtils {


    final static String ProtocolEncoding = "UTF-8";

    /**
     * Read size prefixed string where the size is stored as a 2 byte short.
     *
     * @param buffer The buffer to read from
     */
    public static String readShortString(ByteBuffer buffer) {
        short size = buffer.getShort();
        if (size < 0)
            return null;
        byte[] bytes = new byte[size];
        buffer.get(bytes);
        try {
            return new String(bytes, ProtocolEncoding);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
//
    /**
     * Write a size prefixed string where the size is stored as a 2 byte short
     *
     * @param buffer The buffer to write to
     * @param string The string to write
     */
    public static void writeShortString(ByteBuffer buffer, String string) {
        if (string == null) {
            buffer.putShort((short)-1);
        } else {
            byte[] encodedString;
            try {
                encodedString = string.getBytes(ProtocolEncoding);
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
            if (encodedString.length > Short.MAX_VALUE) {
                throw new KafkaException("String exceeds the maximum size of " + Short.MAX_VALUE + ".");
            } else {
                buffer.putShort((short)encodedString.length);
                buffer.put(encodedString);
            }
        }
    }
//
    /**
     * Return size of a size prefixed string where the size is stored as a 2 byte short
     * @param string The string to write
     */
    public static Integer shortStringLength(String string) {
        if (string == null) {
            return 2;
        } else {
            byte[] encodedString = new byte[0];
            try {
                encodedString = string.getBytes(ProtocolEncoding);
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
            if (encodedString.length > Short.MAX_VALUE) {
                throw new KafkaException("String exceeds the maximum size of " + Short.MAX_VALUE + ".");
            } else {
                return 2 + encodedString.length;
            }
        }
    }

    /**
     * Read an integer out of the bytebuffer from the current position and check that it falls within the given
     * range. If not, throw KafkaException.
     */
    public  static int readIntInRange(ByteBuffer buffer, String name, Tuple<Integer,Integer> range){
        int value = buffer.getInt();
        if (value < range.v1 || value > range.v2)
            throw new KafkaException(name + " has value " + value + " which is not in the range " + range + ".");
        else
            return value;
    }

    /**
     * Read a short out of the bytebuffer from the current position and check that it falls within the given
     * range. If not, throw KafkaException.
     */
    public static short readShortInRange(ByteBuffer buffer, String name, Tuple<Short, Short> range) {
        short value = buffer.getShort();
        if (value < range.v1 || value > range.v2)
            throw new KafkaException(name + " has value " + value + " which is not in the range " + range + ".");
        else
            return value;
    }
//
//    /**
//     * Read a long out of the bytebuffer from the current position and check that it falls within the given
//     * range. If not, throw KafkaException.
//     */
//    public Long
//
//    void readLongInRange(ByteBuffer buffer, String name, range:(Long, Long))
//
//    {
//        val value = buffer.getLong;
//        if (value < range._1 || value > range._2)
//            throw new KafkaException(name + " has value " + value + " which is not in the range " + range + ".");
//        else value;
//    }

}
