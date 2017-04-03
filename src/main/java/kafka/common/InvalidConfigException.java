package kafka.common;

/**
 * Created by Administrator on 2017/4/2.
 */
public class InvalidConfigException extends RuntimeException {
    public InvalidConfigException(String msg) {
        super(msg);
    }
}
