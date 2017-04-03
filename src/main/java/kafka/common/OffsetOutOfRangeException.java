package kafka.common;

/**
 * Created by Administrator on 2017/4/3.
 */
public class OffsetOutOfRangeException extends RuntimeException {
    public OffsetOutOfRangeException(String msg) {
        super(msg);
    }
}
