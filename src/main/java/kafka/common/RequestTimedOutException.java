package kafka.common;

public class RequestTimedOutException extends RuntimeException {
    public RequestTimedOutException(String msg){
        super(msg);
    }
}
