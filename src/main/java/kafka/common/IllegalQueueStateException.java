package kafka.common;

public class IllegalQueueStateException extends RuntimeException {
    public IllegalQueueStateException(String msg){
        super(msg);
    }
}
