package kafka.utils;

import kafka.func.ActionWithP;
import kafka.func.Processor;

import java.util.Iterator;

/**
 * Created by Administrator on 2017/4/6.
 */
public class Itor {
    public static <T> void loop(Iterator<T> it, ActionWithP<T> action){
        while (it.hasNext()){
            action.invoke(it.next());
        }
    }
}
