package kafka.func;/**
 * Created by zhoulf on 2017/5/22.
 */

/**
 * @author
 * @create 2017-05-22 16:41
 **/
public class IntCount {
    private int value;

    private IntCount(int value) {
        this.value = value;
    }

    public static IntCount of(int v) {
        return new IntCount(v);
    }

    public int add(int n) {
        return value += n;
    }

    public int mul(int n){
        return value *= n;
    }

    public int div(int n){
        return value /= n;
    }

    public int minus(int n){
        return value -= n;
    }

    public void set(int n){
        this.value = n;
    }

    public int get(){
        return value;
    }
}
