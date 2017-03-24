package kafka.message;/**
 * Created by zhoulf on 2017/3/22.
 */

import kafka.Scala;
import kafka.utils.Utils;

import java.nio.ByteBuffer;

/**
 * @author
 * @create 2017-03-22 20:03
 **/
public class Message {
    public static final int CrcLength = 4;
    public static final int MagicLength = 1;
    public static final int AttributesLength = 1;
    public static final int KeySizeLength = 4;
    public static final int ValueSizeLength = 4;

    public static final int CrcOffset = 0;
    public static final int MagicOffset = CrcOffset + CrcLength;
    public static final int AttributesOffset = MagicOffset + MagicLength;
    public static final int KeySizeOffset = AttributesOffset + AttributesLength;
    public static final int KeyOffset = KeySizeOffset + KeySizeLength;

    public static final int MessageOverhead = KeyOffset + ValueSizeLength;
    public static final int MinHeaderSize = CrcLength + MagicLength + AttributesLength + KeySizeLength + ValueSizeLength;
    public static final byte CurrentMagicValue = 0;

    public static final int CompressionCodeMask = 0x07;//0111
    public static final int NoCompression = 0;

    public ByteBuffer buffer;

    public Message(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public Message(byte[] bytes,
                   byte[] key,
                   CompressionCodec codec,
                   int payloadOffset,
                   int payloadSize) {
        init(bytes, key, codec, payloadOffset, payloadSize);
    }

    private void init(byte[] bytes, byte[] key, CompressionCodec codec, int payloadOffset, int payloadSize) {
        int playloadSize;
        if (bytes == null) {
            playloadSize = 0;
        } else if (payloadSize >= 0) {
            playloadSize = payloadSize;
        } else {
            playloadSize = bytes.length - payloadOffset;
        }
        buffer = ByteBuffer.allocate(Message.CrcLength +
                Message.MagicLength +
                Message.AttributesLength +
                Message.KeySizeLength +
                ((key == null) ? 0 : key.length) +
                Message.ValueSizeLength + playloadSize);
        // skip crc, we will fill that in at the end
        buffer.position(MagicOffset);
        buffer.put(CurrentMagicValue);
        byte attributes = 0;
        if (codec.codec() > 0) {
            attributes = new Integer(attributes | (CompressionCodeMask & codec.codec())).byteValue();
        }
        buffer.put(attributes);
        if (key == null) {
            buffer.putInt(-1);
        } else {
            buffer.putInt(key.length);
            buffer.put(key, 0, key.length);
        }
        int size;
        if (bytes == null) {
            size = -1;
        } else if (payloadSize >= 0) {
            size = payloadSize;
        } else {
            size = bytes.length - payloadOffset;
        }
        buffer.putInt(size);
        if (bytes != null) {
            buffer.put(bytes, payloadOffset, size);
        }
        buffer.rewind();

        // now compute the checksum and fill it in
        Utils.writeUnsignedInt(buffer, CrcOffset, computeChecksum());
    }

    public Message(byte[] bytes, byte[] key, CompressionCodec codec) {
        init(bytes, key, codec, 0, -1);
    }

    public Message(byte[] bytes, CompressionCodec codec) {
        init(bytes, null, codec, 0, -1);
    }

    public Message(byte[] bytes, byte[] key) {
        init(bytes, key, CompressionCodec.NoCompressionCodec, 0, -1);
    }

    public Message(byte[] bytes) {
        init(bytes, null, CompressionCodec.NoCompressionCodec, 0, -1);
    }

    /**
     * Compute the checksum of the message from the message contents
     */
    public Long computeChecksum() {
        return Utils.crc32(buffer.array(), buffer.arrayOffset() + MagicOffset, buffer.limit() - MagicOffset);
    }

    /**
     * Retrieve the previously computed CRC for this message
     */
    public Long checksum() {
        return Utils.readUnsignedInt(buffer, CrcOffset);
    }

    /**
     * Returns true if the crc stored with the message matches the crc computed off the message contents
     */
    public boolean isValid() {
        return checksum().equals(computeChecksum());
    }

    /**
     * Throw an InvalidMessageException if isValid is false for this message
     */
    public void ensureValid() {
        if (!isValid())
            throw new InvalidMessageException("Message is corrupt (stored crc = " + checksum() + ", computed crc = " + computeChecksum() + ")");
    }

    /**
     * The complete serialized size of this message in bytes (including crc, header attributes, etc)
     */
    public int size() {
        return buffer.limit();
    }

    /**
     * The length of the key in bytes
     */
    public int keySize() {
        return buffer.getInt(Message.KeySizeOffset);
    }

    /**
     * Does the message have a key?
     */
    public boolean hasKey() {
        return keySize() >= 0;
    }

    /**
     * The position where the payload size is stored
     */
    private int payloadSizeOffset() {
        return Message.KeyOffset + Scala.max(0, keySize());
    }

    ;

    /**
     * The length of the message value in bytes
     */
    public int payloadSize() {
        return buffer.getInt(payloadSizeOffset());
    }

    /**
     * Is the payload of this message null
     */
    public boolean isNull() {
        return payloadSize() < 0;
    }

    /**
     * The magic version of this message
     */
    public Byte magic() {
        return buffer.get(MagicOffset);
    }


    /**
     * The attributes stored with this message
     */
    public Byte attributes() {
        return buffer.get(AttributesOffset);
    }

    /**
     * The compression codec used with this message
     */
    public CompressionCodec compressionCodec() {
        return CompressionCodec.getCompressionCodec(buffer.get(AttributesOffset) & CompressionCodeMask);
    }

    /**
     * A ByteBuffer containing the content of the message
     */
    public ByteBuffer payload() {
        return sliceDelimited(payloadSizeOffset());
    }


    /**
     * A ByteBuffer containing the message key
     */
    public ByteBuffer key() {
        return sliceDelimited(KeySizeOffset);
    }


    /**
     * Read a size-delimited byte buffer starting at the given offset
     */
    private ByteBuffer sliceDelimited(int start) {
        int size = buffer.getInt(start);
        if (size < 0) {
            return null;
        } else {
            ByteBuffer b = buffer.duplicate();
            b.position(start + 4);
            b = b.slice();
            b.limit(size);
            b.rewind();
            return b;
        }
    }

    public String toString() {
        return "Message(magic = %d, attributes = %d, crc = %d, key = %s, payload = %s)".
                format(magic().toString(), attributes().toString(), checksum(), key(), payload());
    }

    public boolean equals(Object any) {
        if (any instanceof Message) {
            return this.buffer.equals(any);
        }
        return false;
    }

//    @Override
//    public boolean equals(Object obj) {
//        if (obj instanceof Message) {
//            Message m = (Message) obj;
//            return getSizeInBytes() == m.getSizeInBytes()//
//                    && attributes() == m.attributes()//
//                    && checksum() == m.checksum()//
//                    && payload() == m.payload()//
//                    && magic() == m.magic();
//        }
//        return false;
//    }

    public int hashCode() {
        return buffer.hashCode();
    }

}
