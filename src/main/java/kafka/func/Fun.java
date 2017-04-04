package kafka.func;/**
 * Created by zhoulf on 2017/3/29.
 */

/**
 * @author
 * @create 2017-03-29 14:10
 **/
@FunctionalInterface
public interface Fun<T>{
    T invoke();
}