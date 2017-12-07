package kafka.common;

import kafka.network.Receive;

/**
 * @author zhoulf
 * @create 2017-12-05 17:52
 **/
public class FinalObject<T> {
    private T t;

    private FinalObject() {
    }

    public FinalObject(T t) {
        this.t = t;
    }

    public T get() {
        return t;
    }

    public void set(T t) {
        this.t = t;
    }

    public static FinalObject of() {
        return new FinalObject(null);
    }

    public static <T> FinalObject<T> of(T t) {
        return new FinalObject<>(t);
    }
}
