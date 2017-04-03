package kafka.common;

/**
 * Created by Administrator on 2017/4/3.
 */
public class InvalidMessageSizeException extends RuntimeException {
    public InvalidMessageSizeException(String msg) {
        super(msg);
    }
}
