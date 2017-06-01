package kafka.utils;

import com.google.common.collect.Lists;
import kafka.func.ActionWithParam;
import kafka.func.Handler;

import java.util.Iterator;
import java.util.List;

/**
 * Created by Administrator on 2017/4/6.
 */
public class Itor {
    public static <T> void loop(Iterator<T> it, ActionWithParam<T> action) {
        while (it.hasNext()) {
            action.invoke(it.next());
        }
    }

    public static <T> List<T> filter(Iterator<T> it, Handler<T, Boolean> action) {
        List<T> list = Lists.newArrayList();
        while (it.hasNext()) {
            T t = it.next();
            if (action.handle(t)) {
                list.add(t);
            }
        }
        return list;
    }
}
