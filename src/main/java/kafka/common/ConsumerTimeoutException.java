package kafka.common;

public class ConsumerTimeoutException extends RuntimeException {
    public ConsumerTimeoutException(String msg){
        super(msg);
    }
    public ConsumerTimeoutException(){
        super();
    }
}
