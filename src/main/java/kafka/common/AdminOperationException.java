package kafka.common;

public class AdminOperationException extends RuntimeException {
    public AdminOperationException(String msg){
        super(msg);
    }
}
