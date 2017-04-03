package kafka.func;

/**
 * Created by Administrator on 2017/4/3.
 */
public class Tuple<V1, V2> {
    public V1 v1;
    public V2 v2;

    public Tuple(V1 v1, V2 v2) {
        this.v1 = v1;
        this.v2 = v2;
    }

    public static <V1, V2> Tuple<V1, V2> of(V1 v1, V2 v2) {
        return new Tuple<V1, V2>(v1, v2);
    }
}
