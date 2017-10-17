package kafka.func;/**
 * Created by zhoulf on 2017/3/29.
 */

/**
 * @author
 * @create 2017-03-29 14:10
 **/
@FunctionalInterface
public interface Handler2<P1,P2,V>{
    V handle(P1 p1,P2 p2);
}