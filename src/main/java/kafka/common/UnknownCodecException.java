package kafka.common;/**
 * Created by zhoulf on 2017/3/22.
 */

/**
 * @author
 * @create 2017-03-22 21:11
 **/
public class UnknownCodecException extends RuntimeException {
    public UnknownCodecException(String msg){
        super(msg);
    }
}
