package kafka.common;

public class ProducerClosedException extends RuntimeException {
    public ProducerClosedException(String msg) {
        super(msg);
    }

    public ProducerClosedException() {
        super();
    }
}
