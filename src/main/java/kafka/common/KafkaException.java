package kafka.common;

/**
 * Created by Administrator on 2017/3/27.
 */
public class KafkaException extends RuntimeException {
    private String message;
    private Throwable t;

    public KafkaException(String message, Throwable t) {
        super(message, t);
        this.message = message;
        this.t = t;
    }

    public KafkaException(String message) {
        this(message, null);
    }

    public KafkaException(Throwable t) {
        this("", t);
    }
}
