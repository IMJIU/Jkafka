package kafka.common;

public class ConsumerCoordinatorNotAvailableException extends RuntimeException {
    public ConsumerCoordinatorNotAvailableException(String msg){
        super(msg);
    }
}
