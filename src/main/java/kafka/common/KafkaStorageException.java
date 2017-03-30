package kafka.common;

/**
 * Created by Administrator on 2017/3/27.
 */
public class KafkaStorageException extends RuntimeException {
    private String message;
    private Throwable t;

    public KafkaStorageException(String message, Throwable t) {
        super(message, t);
        this.message = message;
        this.t = t;
    }

    public KafkaStorageException(String message) {
        this(message, null);
    }

    public KafkaStorageException(Throwable t) {
        this("", t);
    }
}
