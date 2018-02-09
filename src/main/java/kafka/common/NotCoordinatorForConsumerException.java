package kafka.common;

public class NotCoordinatorForConsumerException extends RuntimeException {
    public NotCoordinatorForConsumerException(String msg){
        super(msg);
    }
}
