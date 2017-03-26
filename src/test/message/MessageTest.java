package message;

import com.google.common.collect.Lists;
import kafka.message.CompressionCodec;
import kafka.message.Message;
import kafka.utils.Utils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2017/3/26.
 */


public class MessageTest {

    public List<MessageTestVal> messages = Lists.newArrayList();

    @Before
    public void setUp() {

        List<byte[]> keys = Lists.newArrayList();
        keys.add(null);
        keys.add("key".getBytes());
        keys.add("".getBytes());
        List<byte[]> vals = Lists.newArrayList();
        vals.add(null);
        vals.add("value".getBytes());
        vals.add("".getBytes());
        CompressionCodec[] codec = CompressionCodec.values();
        for (int i = 0; i < keys.size(); i++) {
            messages.add(new MessageTestVal(keys.get(i), vals.get(i), codec[i], new Message(vals.get(i), keys.get(i), codec[i])));
        }
    }

    @Test
    public void testFieldValues() {
        for (MessageTestVal v : messages) {
            if (v.payload == null) {
                Assert.assertTrue(v.message.isNull());
                Assert.assertEquals("Payload should be null", null, v.message.payload());
            } else {
                TestUtils.checkEquals(ByteBuffer.wrap(v.payload), v.message.payload());
            }
            Assert.assertEquals(Message.CurrentMagicValue, v.message.magic());
            if (v.message.hasKey())
                TestUtils.checkEquals(ByteBuffer.wrap(v.key), v.message.key());
            else
                Assert.assertEquals(null, v.message.key());
            Assert.assertEquals(v.codec, v.message.compressionCodec());
        }
    }

    @Test
    public void testChecksum() {
        for (MessageTestVal v : messages) {
            Assert.assertTrue("Auto-computed checksum should be valid", v.message.isValid());
            // garble checksum
            int badChecksum = (int) (v.message.checksum() + 1 % Integer.MAX_VALUE);
            Utils.writeUnsignedInt(v.message.buffer, Message.CrcOffset, (long) badChecksum);
            Assert.assertFalse("Message with invalid checksum should be invalid", v.message.isValid());
        }
    }

    @Test
    public void testEquality() {
        for (MessageTestVal v : messages) {
            Assert.assertFalse("Should not equal null", v.message.equals(null));
            Assert.assertFalse("Should not equal a random string", v.message.equals("asdf"));
            Assert.assertTrue("Should equal itself", v.message.equals(v.message));
            Message copy = new Message(v.payload,v.key, v.codec);
            Assert.assertTrue("Should equal another message with the same content.", v.message.equals(copy));
        }
    }

    @Test
    public void testIsHashable() {
        // this is silly, but why not
        Map<Message,Message> m = new HashMap<kafka.message.Message, kafka.message.Message>();
        for (MessageTestVal v:messages)
            m.put(v.message, v.message);
        for (MessageTestVal v:messages)
            Assert.assertEquals(v.message, m.get(v.message));
    }
}

class MessageTestVal {
    public byte[] key;
    public byte[] payload;
    public CompressionCodec codec;
    public Message message;

    public MessageTestVal(byte[] key, byte[] payload, CompressionCodec codec, Message message) {
        this.key = key;
        this.payload = payload;
        this.codec = codec;
        this.message = message;
    }

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }

    public CompressionCodec getCodec() {
        return codec;
    }

    public void setCodec(CompressionCodec codec) {
        this.codec = codec;
    }

    public Message getMessage() {
        return message;
    }

    public void setMessage(Message message) {
        this.message = message;
    }
}