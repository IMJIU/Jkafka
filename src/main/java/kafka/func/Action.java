package kafka.func;/**
 * Created by zhoulf on 2017/3/29.
 */

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author
 * @create 2017-03-29 12:44
 **/
@FunctionalInterface
public interface Action {
    void invoke() throws Throwable;
}
