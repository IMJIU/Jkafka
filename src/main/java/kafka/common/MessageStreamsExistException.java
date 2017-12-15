package kafka.common;

public class MessageStreamsExistException extends RuntimeException {
    public MessageStreamsExistException(String msg) {
        super(msg);
    }

    public MessageStreamsExistException(String msg, Throwable e) {
        super(msg, e);
    }
}
