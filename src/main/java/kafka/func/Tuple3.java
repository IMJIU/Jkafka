package kafka.func;

/**
 * Created by Administrator on 2017/4/3.
 */
public class Tuple3<A, B, C> {
    public A v1;
    public B v2;
    public C v3;

    public Tuple3(A v1, B v2, C v3) {
        this.v1 = v1;
        this.v2 = v2;
        this.v3 = v3;
    }

    public static <A, B, C> Tuple3<A, B, C> of(A v1, B v2, C v3) {
        return new Tuple3<A, B, C>(v1, v2, v3);
    }
}
