package message;

import kafka.message.ByteBufferMessageSet;
import kafka.message.CompressionCodec;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import kafka.utils.IteratorTemplate;
import org.junit.Assert;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Random;

/**
 * Created by Administrator on 2017/3/26.
 */
public class TestUtils {
    public final static String IoTmpDir = "f:\\temp\\";
    public final static String Letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    public final static String Digits = "0123456789";
    public final static String LettersAndDigits = Letters + Digits;

    /* A consistent random number generator to make tests repeatable */
    public static Random seededRandom = new Random(192348092834L);
    public static Random random = new Random();

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

    public static void assertEquals(String msg, Iterator<MessageAndOffset> expected, Iterator<MessageAndOffset> actual) {
        checkEquals(expected, actual);
    }

    /**
     * Throw an exception if the two iterators are of differing lengths or contain
     * different messages on their Nth element
     */
    public static <T> void checkEquals(Iterator<T> expected, Iterator<T> actual) {
        int length = 0;
        while (expected.hasNext() && actual.hasNext()) {
            length += 1;
//            System.out.println(expected.next());
//            System.out.println(actual.next());
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

    public static ByteBufferMessageSet singleMessageSet(byte[] payload) {
        byte[] key = null;
        return new ByteBufferMessageSet(CompressionCodec.NoCompressionCodec, new Message(payload, key));
    }


    public static void print(Iterator<MessageAndOffset> iterator) {
        System.out.println("================");
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }
        System.out.println("================");
    }


    public static void writeNonsenseToFile(File fileName, Long position, Integer size) throws Exception {
        RandomAccessFile file = new RandomAccessFile(fileName, "rw");
        file.seek(position);
        for (int i = 0; i < size; i++)
            file.writeByte(random.nextInt(255));
        file.close();
    }
}
