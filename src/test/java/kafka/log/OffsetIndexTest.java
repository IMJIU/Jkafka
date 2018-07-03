package kafka.log;

import com.google.common.collect.Lists;
import kafka.common.InvalidOffsetException;
import kafka.utils.Sc;
import kafka.utils.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author
 * @create 2017-03-30 36 10
 **/
public class OffsetIndexTest {
    OffsetIndex idx = null;
    Integer maxEntries = 30;

    @Before
    public void setup() throws IOException {
        this.idx = new OffsetIndex(nonExistantTempFile(), 45L, 30 * 8);
    }

    @After
    public void teardown() {
        if (this.idx != null) ;
        this.idx.file.delete();
    }

    /**
     * 测试随机查询
     */
    @Test
    public void randomLookupTest() {
        //没有找到返回offset 0
        Assert.assertEquals("Not present value should return physical offset 0.", new OffsetPosition(idx.baseOffset, 0), idx.lookup(92L));

        // append some random values;
        Integer base = idx.baseOffset.intValue() + 1;
        Integer size = idx.maxEntries;
        List<Long> vals = Sc.map(monotonicList(base, size), n -> n.longValue());//顺序 随机数
        List<Integer> vals2 = monotonicList(0, size);
        for (int i = 0; i < vals.size(); i++) {
            idx.append(vals.get(i), vals2.get(i));
        }

        // should be able to find all those values; 二分法查找所有的offset 一致
        for (int i = 0; i < vals.size(); i++) {
            Long logical = vals.get(i);
            Integer physical = vals2.get(i);
            Assert.assertEquals("Should be able to find values that are present.", new OffsetPosition(logical, physical), idx.lookup(logical));
        }

        // for non-present values we should find the offset of the largest value less than or equal to this;
        TreeMap<Long, OffsetPosition> valMap = new TreeMap<>();
        for (int i = 0; i < vals.size(); i++) {
            valMap.put(vals.get(i), new OffsetPosition(vals.get(i), vals2.get(i)));
        }
        //生成baseOffset 到 max的offsetList
        List<Long> offsets = Sc.itToList(idx.baseOffset.intValue(), vals.get(vals.size() - 1).intValue(), n -> (long) (n + 1));
        long max = offsets.get(offsets.size() - 1);
        //打乱offsetList
        Collections.shuffle(offsets);
        //遍历，直到最大值（每次遍历次数不一样）
        for (long offset = offsets.get(0); offset < max; offset++) {
            OffsetPosition rightAnswer;
            if (offset < valMap.firstKey()) {
                rightAnswer = new OffsetPosition(idx.baseOffset, 0);
            } else {
                rightAnswer = valMap.get(valMap.floorKey(offset));
            }
            //因为二分法查询不会精确到正好有这个key 所以是floorKey
            Assert.assertEquals("The index should give the same answer as the sorted map", rightAnswer, idx.lookup(offset));
        }
    }

    /**
     * 查询极端测试 首尾
     */
    @Test
    public void lookupExtremeCases() {
        Assert.assertEquals("Lookup on empty file", new OffsetPosition(idx.baseOffset, 0), idx.lookup(idx.baseOffset));
        for (int i = 0; i < idx.maxEntries; i++)
            idx.append(idx.baseOffset + i + 1, i);
        // check first and last entry;
        Assert.assertEquals(new OffsetPosition(idx.baseOffset, 0), idx.lookup(idx.baseOffset));
        Assert.assertEquals(new OffsetPosition(idx.baseOffset + idx.maxEntries, idx.maxEntries - 1), idx.lookup(idx.baseOffset + idx.maxEntries));
    }

    /**
     * 添加超过限制值
     */
    @Test
    public void appendTooMany() {
        for (int i = 0; i < idx.maxEntries; i++) {
            Long offset = idx.baseOffset + i + 1;
            idx.append(offset, i);
        }
        assertWriteFails("Append should fail on a full index", idx, idx.maxEntries + 1, IllegalArgumentException.class);
    }

    /**
     * offset只能升序添加
     */
    @Test(expected = InvalidOffsetException.class)
    public void appendOutOfOrder() {
        idx.append(51L, 0);
        idx.append(50L, 1);
    }

    /**
     * 测试关闭之后再打开
     * 但是关闭后打开不可再增加offset
     */
    @Test
    public void testReopen() throws IOException {
        OffsetPosition first = new OffsetPosition(51L, 0);
        OffsetPosition sec = new OffsetPosition(52L, 1);
        idx.append(first.offset, first.position);
        idx.append(sec.offset, sec.position);
        idx.close();
        OffsetIndex idxRo = new OffsetIndex(idx.file, idx.baseOffset);
        Assert.assertEquals(first, idxRo.lookup(first.offset));
        Assert.assertEquals(sec, idxRo.lookup(sec.offset));
        Assert.assertEquals(sec.offset, idxRo.lastOffset);
        Assert.assertEquals(new Integer(2), idxRo.entries());

        //关闭后，用只读构造函数打开，不可再添加
        assertWriteFails("Append should fail on read-only index", idxRo, 53, IllegalArgumentException.class);
    }

    /**
     * 截断
     */
    @Test
    public void truncate() throws IOException {
        OffsetIndex idx = new OffsetIndex(nonExistantTempFile(), 0L, 10 * 8);
        idx.truncate();
        for (int i = 1; i < 10; i++)
            idx.append(new Long(i), i);

        // now check the last offset after various truncate points and validate that we can still append to the index.;
        idx.truncateTo(12L);
        Assert.assertEquals("Index should be unchanged by truncate past the end", new OffsetPosition(9L, 9), idx.lookup(10L));
        Assert.assertEquals("9 should be the last entry in the index", new Long(9), idx.lastOffset);

        idx.append(10L, 10);
        idx.truncateTo(10L);//10也会删除
        Assert.assertEquals("Index should be unchanged by truncate at the end", new OffsetPosition(9L, 9), idx.lookup(10L));
        Assert.assertEquals("9 should be the last entry in the index", new Long(9), idx.lastOffset);
        idx.append(10L, 10);

        idx.truncateTo(9L);
        Assert.assertEquals("Index should truncate off last entry", new OffsetPosition(8l, 8), idx.lookup(10L));
        Assert.assertEquals("8 should be the last entry in the index", new Long(8), idx.lastOffset);
        idx.append(9L, 9);

        idx.truncateTo(5L);
        Assert.assertEquals("4 should be the last entry in the index", new OffsetPosition(4L, 4), idx.lookup(10l));
        Assert.assertEquals("4 should be the last entry in the index", new Long(4), idx.lastOffset);
        idx.append(5L, 5);

        idx.truncate();
        Assert.assertEquals("Full truncation should leave no entries", new Integer(0), idx.entries());
        idx.append(0L, 0);
    }

    public <T> void assertWriteFails(String message, OffsetIndex idx, Integer offset, Class<T> klass) {
        try {
            idx.append(offset.longValue(), 1);
            System.out.println("can not be there!");
            throw new Exception(message);
        } catch (Throwable e) {
            Assert.assertEquals("Got an unexpected exception.", klass, e.getClass());
        }
    }

    /**
     * 随机数list
     */
    public List<Integer> monotonicList(Integer base, Integer len) {
        Random rand = new Random(1L);
        List<Integer> vals = Lists.newArrayList();
        Integer last = base;
        for (int i = 0; i < len; i++) {
            last += rand.nextInt(15) + 1;
            vals.add(last);
        }
        return vals;
    }

    public File nonExistantTempFile() {
        File file = TestUtils.tempFile();
        file.delete();
        return file;
    }
}
