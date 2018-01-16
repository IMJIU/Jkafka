package kafka.utils;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import jdk.nashorn.internal.ir.annotations.Immutable;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author zhoulf
 * @create 2018-01-16 13:55
 **/
public class JsonTest {

    @Test
    public void testJsonEncoding() {
        assertEquals("null", JSON.toJSONString(null));
        assertEquals("1", JSON.toJSONString(1));
        assertEquals("1", JSON.toJSONString(1L));
        assertEquals("1", JSON.toJSONString((byte) 1));
        assertEquals("1", JSON.toJSONString((short) 1));
        assertEquals("1.0", JSON.toJSONString(1.0));
        assertEquals("\"str\"", JSON.toJSONString("str"));
        assertEquals("true", JSON.toJSONString(true));
        assertEquals("false", JSON.toJSONString(false));
        assertEquals("[]", JSON.toJSONString(Lists.newArrayList()));
        assertEquals("[1,2,3]", JSON.toJSONString(Lists.newArrayList(1, 2, 3)));
        assertEquals("[1,\"2\",[3]]", JSON.toJSONString(Lists.newArrayList(1, "2", Lists.newArrayList(3))));
        assertEquals("{}", JSON.toJSONString(Maps.newHashMap()));
        assertEquals("{\"a\":1,\"b\":2}", JSON.toJSONString(ImmutableMap.of("a", 1, "b", 2)));
        assertEquals("{\"a\":[1,2],\"c\":[3,4]}", JSON.toJSONString(ImmutableMap.of("a", Lists.newArrayList(1, 2), "c", Lists.newArrayList(3, 4))));
    }
}
