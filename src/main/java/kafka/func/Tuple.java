package kafka.func;

/**
 * Created by Administrator on 2017/4/3.
 */
public class Tuple<A, B> {
    public final static Tuple EMPTY = new Tuple();

    public A v1;
    public B v2;

    public boolean isEmpty() {
        if (this == EMPTY) {
            return true;
        }
        return false;
    }

    public Tuple() {
    }

    public Tuple(A v1, B v2) {
        this.v1 = v1;
        this.v2 = v2;
    }

    public static <A, B> Tuple<A, B> of(A v1, B v2) {
        return new Tuple<A, B>(v1, v2);
    }
}
