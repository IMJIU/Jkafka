package kafka.func;/**
 * Created by zhoulf on 2017/3/29.
 */

/**
 * @author
 * @create 2017-03-29 12:44
 **/
@FunctionalInterface
public interface ActionP2<P,P2> {
    void invoke(P p1,P2 p2);
}
