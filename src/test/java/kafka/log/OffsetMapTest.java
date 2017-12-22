package kafka.log;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * @author zhoulf
 * @create 2017-12-22 36 10
 **/

public class OffsetMapTest {
    public void main(String[] args) {
        if (args.length != 2) {
            System.err.println("java USAGE OffsetMapTest size load");
            System.exit(1);
        }
        OffsetMapTest test = new OffsetMapTest();
        Integer size = Integer.parseInt(args[0]);
        Double load = Double.parseDouble(args[1]);
        long start = System.nanoTime();
        SkimpyOffsetMap map = test.validateMap(size, load);
        double ellapsedMs = (System.nanoTime() - start) / 1000.0 / 1000.0;
        System.out.println(map.size() + " entries in map of size " + map.slots() + " in " + ellapsedMs + " ms");
        System.out.println(String.format("Collision rate: %.1f%%", 100 * map.collisionRate()));
    }

    @Test
    public void testBasicValidation() {
        validateMap(10);
        validateMap(100);
        validateMap(1000);
        validateMap(5000);
    }

    @Test
    public void testClear() {
        SkimpyOffsetMap map = new SkimpyOffsetMap(4000);
        for (Integer i = 0; i < 10; i++)
            map.put(key(i), i.longValue());
        for (int i = 0; i < 10; i++)
            Assert.assertEquals(new Long(i), map.get(key(i)));
        map.clear();
        for (Integer i = 0; i < 10; i++)
            Assert.assertEquals(map.get(key(i)), new Long(-1L));
    }

    public ByteBuffer key(Integer key) {
        return ByteBuffer.wrap(key.toString().getBytes());
    }

    public SkimpyOffsetMap validateMap(Integer items) {
        return validateMap(items, null);
    }

    public SkimpyOffsetMap validateMap(Integer items, Double loadFactor) {
        if (loadFactor == null) loadFactor = 0.5d;
        SkimpyOffsetMap map = new SkimpyOffsetMap(new Double(items / loadFactor * 24).intValue());
        for (Integer i = 0; i < items; i++)
            map.put(key(i), i.longValue());
        int misses = 0;
        for (int i = 0; i < items; i++)
            Assert.assertEquals(map.get(key(i)), new Long(i));
        return map;
    }

}

