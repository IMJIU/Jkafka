package kafka.common;

public class ConsumerCoordinatorNotAvailableException extends RuntimeException {
    public ConsumerCoordinatorNotAvailableException() {
        super();
    }
    public ConsumerCoordinatorNotAvailableException(String msg){
        super(msg);
    }
}
