package message;

import kafka.message.Message;
import kafka.message.MessageAndOffset;
import kafka.utils.IteratorTemplate;
import org.junit.Assert;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * Created by Administrator on 2017/3/26.
 */
public class TestUtils {
    public static void checkEquals(ByteBuffer b1, ByteBuffer b2) {
        Assert.assertEquals("Buffers should have equal length", b1.limit() - b1.position(), b2.limit() - b2.position());
        for (int i = 0; i < b1.limit() - b1.position(); i++)
            Assert.assertEquals("byte " + i + " byte not equal.", b1.get(b1.position() + i), b2.get(b1.position() + i));
    }

    /**
     * Create a temporary file
     */
    public static File tempFile() {
        File f = null;
        try {
            f = File.createTempFile("kafka", ".tmp");
            f.deleteOnExit();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return f;
    }

    /**
     * Throw an exception if the two iterators are of differing lengths or contain
     * different messages on their Nth element
     */
    public static <T> void checkEquals(Iterator<T> expected, Iterator<T> actual) {
        int length = 0;
        while (expected.hasNext() && actual.hasNext()) {
            length += 1;
            Assert.assertEquals(expected.next(), actual.next());
        }

        // check if the expected iterator is longer
        if (expected.hasNext()) {
            int length1 = length;
            while (expected.hasNext()) {
                expected.next();
                length1 += 1;
            }
            Assert.assertFalse("Iterators have uneven length-- first has more: " + length1 + " > " + length, true);
        }

        // check if the actual iterator was longer
        if (actual.hasNext()) {
            int length2 = length;
            while (actual.hasNext()) {
                actual.next();
                length2 += 1;
            }
            Assert.assertFalse("Iterators have uneven length-- second has more: " + length2 + " > " + length, true);
        }
    }

    @SuppressWarnings("unchecked")
    public static Iterator<Message> getMessageIterator(final Iterator<MessageAndOffset> iterator) {
        return new IteratorTemplate<Message>() {
            @Override
            protected Message makeNext() {
                if (iterator.hasNext())
                    return iterator.next().message;
                else
                    return allDone();
            }
        };
    }
}
