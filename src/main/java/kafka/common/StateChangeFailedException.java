package kafka.common;

public class StateChangeFailedException extends RuntimeException {
    public StateChangeFailedException(String msg){
        super(msg);
    }

    public StateChangeFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
