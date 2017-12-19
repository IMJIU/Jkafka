package kafka.common;

public class ConsumerRebalanceFailedException extends RuntimeException {
    public ConsumerRebalanceFailedException(String msg){
        super(msg);
    }
}
