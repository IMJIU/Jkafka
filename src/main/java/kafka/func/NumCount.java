package kafka.func;/**
 * Created by zhoulf on 2017/5/22.
 */

/**
 * @author
 * @create 2017-05-22 16:41
 **/
public class NumCount<T extends Number> {
    private T value;

    private NumCount(T value) {
        this.value = value;
    }

    public static<T extends Number> NumCount of(T v) {
        return new NumCount(v);
    }

    public void set(T n){
        this.value = n;
    }

    public T get(){
        return value;
    }
}
