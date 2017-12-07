package kafka.common;

public class UnavailableProducerException extends RuntimeException {
    public UnavailableProducerException(String msg){
        super(msg);
    }
}
