package kafka.common;/**
 * Created by zhoulf on 2017/3/29.
 */

/**
 * @author
 * @create 2017-03-29 14:07
 **/
public class InvalidOffsetException extends RuntimeException {
    public InvalidOffsetException(String msg){
        super(msg);
    }
}
