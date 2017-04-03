package kafka.common;

/**
 * Created by Administrator on 2017/4/3.
 */
public class MessageSetSizeTooLargeException  extends RuntimeException {
    public MessageSetSizeTooLargeException(String msg) {
        super(msg);
    }
}
