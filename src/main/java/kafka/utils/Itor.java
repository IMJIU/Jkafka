package kafka.utils;

import kafka.func.ActionWithParam;

import java.util.Iterator;

/**
 * Created by Administrator on 2017/4/6.
 */
public class Itor {
    public static <T> void loop(Iterator<T> it, ActionWithParam<T> action){
        while (it.hasNext()){
            action.invoke(it.next());
        }
    }
}
