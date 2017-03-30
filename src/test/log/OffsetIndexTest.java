package log;/**
 * Created by zhoulf on 2017/3/30.
 */

import kafka.log.OffsetIndex;
import message.TestUtils;
import org.junit.After;
import org.junit.Before;

import java.io.File;

/**
 * @author
 * @create 2017-03-30 36 10
 **/
public class OffsetIndexTest {
    OffsetIndex idx = null;
    Integer maxEntries = 30;

    @Before
    public void setup() {
        this.idx = new OffsetIndex(nonExistantTempFile(), 45L, 30 * 8);
    }
    @After
    public void teardown() {
        if(this.idx != null)
            this.idx.file.delete();
    }
//
//    @Test;
//    def randomLookupTest() {
//        assertEquals("Not present value should return physical offset 0.", OffsetPosition(idx.baseOffset, 0), idx.lookup(92L));
//
//        // append some random values;
//        val base = idx.baseOffset.toInt + 1;
//        val size = idx.maxEntries;
//        val Seq vals[(Long, Int)] = monotonicSeq(base, size).map(_.toLong).zip(monotonicSeq(0, size));
//        vals.foreach{x => idx.append(x._1, x._2)}
//
//        // should be able to find all those values;
//        for((logical, physical) <- vals);
//        assertEquals("Should be able to find values that are present.", OffsetPosition(logical, physical), idx.lookup(logical));
//
//        // for non-present values we should find the offset of the largest value less than or equal to this;
//        val valMap = new immutable.TreeMap[Long, (Long, Int)]() ++ vals.map(p => (p._1, p));
//        val offsets = (idx.baseOffset until vals.last._1.toInt).toArray;
//        Collections.shuffle(Arrays.asList(offsets));
//        for(offset <- offsets.take(30)) {
//            val rightAnswer =
//            if(offset < valMap.firstKey)
//                OffsetPosition(idx.baseOffset, 0);
//            else;
//                OffsetPosition(valMap.to(offset).last._1, valMap.to(offset).last._2._2);
//            assertEquals("The index should give the same answer as the sorted map", rightAnswer, idx.lookup(offset));
//        }
//    }
//
//    @Test;
//    def lookupExtremeCases() {
//        assertEquals("Lookup on empty file", OffsetPosition(idx.baseOffset, 0), idx.lookup(idx.baseOffset));
//        for(i <- 0 until idx.maxEntries);
//        idx.append(idx.baseOffset + i + 1, i);
//        // check first and last entry;
//        assertEquals(OffsetPosition(idx.baseOffset, 0), idx.lookup(idx.baseOffset));
//        assertEquals(OffsetPosition(idx.baseOffset + idx.maxEntries, idx.maxEntries - 1), idx.lookup(idx.baseOffset + idx.maxEntries));
//    }
//
//    @Test;
//    def appendTooMany() {
//        for(i <- 0 until idx.maxEntries) {
//            val offset = idx.baseOffset + i + 1;
//            idx.append(offset, i);
//        }
//        assertWriteFails("Append should fail on a full index", idx, idx.maxEntries + 1, classOf[IllegalArgumentException]);
//    }
//
//    @Test(expected = classOf[InvalidOffsetException]);
//    def appendOutOfOrder() {
//        idx.append(51, 0);
//        idx.append(50, 1);
//    }
//
//    @Test;
//    def testReopen() {
//        val first = OffsetPosition(51, 0);
//        val sec = OffsetPosition(52, 1);
//        idx.append(first.offset, first.position);
//        idx.append(sec.offset, sec.position);
//        idx.close();
//        val idxRo = new OffsetIndex(file = idx.file, baseOffset = idx.baseOffset);
//        assertEquals(first, idxRo.lookup(first.offset));
//        assertEquals(sec, idxRo.lookup(sec.offset));
//        assertEquals(sec.offset, idxRo.lastOffset);
//        assertEquals(2, idxRo.entries);
//        assertWriteFails("Append should fail on read-only index", idxRo, 53, classOf[IllegalArgumentException]);
//    }
//
//    @Test;
//    def truncate() {
//        val idx = new OffsetIndex(file = nonExistantTempFile(), baseOffset = 0L, maxIndexSize = 10 * 8);
//        idx.truncate();
//        for(i <- 1 until 10);
//        idx.append(i, i);
//
//        // now check the last offset after various truncate points and validate that we can still append to the index.;
//        idx.truncateTo(12);
//        assertEquals("Index should be unchanged by truncate past the end", OffsetPosition(9, 9), idx.lookup(10));
//        assertEquals("9 should be the last entry in the index", 9, idx.lastOffset);
//
//        idx.append(10, 10);
//        idx.truncateTo(10);
//        assertEquals("Index should be unchanged by truncate at the end", OffsetPosition(9, 9), idx.lookup(10));
//        assertEquals("9 should be the last entry in the index", 9, idx.lastOffset);
//        idx.append(10, 10);
//
//        idx.truncateTo(9);
//        assertEquals("Index should truncate off last entry", OffsetPosition(8, 8), idx.lookup(10));
//        assertEquals("8 should be the last entry in the index", 8, idx.lastOffset);
//        idx.append(9, 9);
//
//        idx.truncateTo(5);
//        assertEquals("4 should be the last entry in the index", OffsetPosition(4, 4), idx.lookup(10));
//        assertEquals("4 should be the last entry in the index", 4, idx.lastOffset);
//        idx.append(5, 5);
//
//        idx.truncate();
//        assertEquals("Full truncation should leave no entries", 0, idx.entries());
//        idx.append(0, 0);
//    }
//
//    def assertWriteFails[T](String message, OffsetIndex idx, Int offset, Class klass[T]) {
//        try {
//            idx.append(offset, 1);
//            fail(message);
//        } catch {
//            case Exception e => assertEquals("Got an unexpected exception.", klass, e.getClass);
//        }
//    }
//
//    def monotonicSeq(Int base, Int len): Seq[Int] = {
//        val rand = new Random(1L);
//        val vals = new mutable.ArrayBuffer[Int](len);
//                var last = base;
//        for (i <- 0 until len) {
//            last += rand.nextInt(15) + 1;
//            vals += last;
//        }
//        vals;
//    }
//
    public File nonExistantTempFile() {
        File file = TestUtils.tempFile();
        file.delete();
        return file;
    }
}
