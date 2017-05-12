package kafka.func;/**
 * Created by zhoulf on 2017/3/29.
 */

/**
 * @author
 * @create 2017-03-29 14:10
 **/
@FunctionalInterface
public interface Checker<P1, P2, B> {
    B check(P1 p1, P2 p2);
}