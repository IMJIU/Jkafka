package kafka.api;

import kafka.common.KafkaException;
import kafka.utils.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Random;

/**
 * Created by Administrator on 2017/12/17.
 */

public class ApiUtilsTest {
    public static final Random rnd = new Random();

    @Test
    public void testShortStringNonASCII() {
        // Random-length strings;
        for (int i = 0; i <= 100; i++) {
            // Since we're using UTF-8 encoding, each encoded byte will be one to four bytes long;
            String s = TestUtils.nextString(Math.abs(ApiUtilsTest.rnd.nextInt()) % (Short.MAX_VALUE / 4));
            ByteBuffer bb = ByteBuffer.allocate(ApiUtils.shortStringLength(s));
            ApiUtils.writeShortString(bb, s);
            bb.rewind();
            Assert.assertEquals(s, ApiUtils.readShortString(bb));
        }
    }

    @Test
    public void testShortStringASCII() {
        // Random-length strings;
        for (int i = 0; i <= 100; i++) {
            String s = TestUtils.nextString(Math.abs(ApiUtilsTest.rnd.nextInt()) % Short.MAX_VALUE);
            ByteBuffer bb = ByteBuffer.allocate(ApiUtils.shortStringLength(s));
            ApiUtils.writeShortString(bb, s);
            bb.rewind();
            Assert.assertEquals(s, ApiUtils.readShortString(bb));
        }

        // Max size string;
        String s1 = TestUtils.nextString(new Short(Short.MAX_VALUE).intValue());
        ByteBuffer bb = ByteBuffer.allocate(ApiUtils.shortStringLength(s1));
        ApiUtils.writeShortString(bb, s1);
        bb.rewind();
        Assert.assertEquals(s1, ApiUtils.readShortString(bb));

        // One byte too big;
        String s2 = TestUtils.nextString(Short.MAX_VALUE + 1);
        try {
            ApiUtils.shortStringLength(s2);
            throw new RuntimeException("fail");
        } catch (KafkaException e) {
            // ok;
        }
        try {
            ApiUtils.writeShortString(bb, s2);
            throw new RuntimeException("fail");
        } catch (KafkaException e) {
            // ok;
        }
    }
}
