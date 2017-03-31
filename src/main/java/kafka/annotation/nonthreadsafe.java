package kafka.annotation;/**
 * Created by zhoulf on 2017/3/31.
 */

import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

/**
 * @author
 * @create 2017-03-31 23:47
 **/
@Target({ElementType.TYPE,ElementType.METHOD})
public @interface nonthreadsafe {
}
