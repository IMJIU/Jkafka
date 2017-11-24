package kafka.common;

/**
 * Created by Administrator on 2017/4/3.
 */
public class ReplicaNotAvailableException extends RuntimeException {
    public ReplicaNotAvailableException(String msg) {
        super(msg);
    }

    public ReplicaNotAvailableException(Throwable cause) {
        super(cause);
    }
}
