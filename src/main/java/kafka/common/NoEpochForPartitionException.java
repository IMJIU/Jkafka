package kafka.common;/**
 * Created by zhoulf on 2017/5/15.
 */

/**
 * @author
 * @create 2017-05-15 15:28
 **/
public class NoEpochForPartitionException extends RuntimeException {
    public NoEpochForPartitionException(String msg) {
        super(msg);
    }
}
