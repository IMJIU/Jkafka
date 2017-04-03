package kafka.common;

/**
 * Created by Administrator on 2017/4/2.
 */
public class MessageSizeTooLargeException extends RuntimeException {
    public MessageSizeTooLargeException(String msg) {
        super(msg);
    }
}
