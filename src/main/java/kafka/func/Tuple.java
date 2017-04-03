package kafka.func;

/**
 * Created by Administrator on 2017/4/3.
 */
public class Tuple<K,V>{
    public K k;
    public V v;

    public Tuple(K k, V v) {
        this.k = k;
        this.v = v;
    }
}
