package kafka.common;

public class FailedToSendMessageException extends RuntimeException {
    public FailedToSendMessageException(String msg){
        super(msg);
    }
}
