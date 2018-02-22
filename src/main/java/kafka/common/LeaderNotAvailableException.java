package kafka.common;

public class LeaderNotAvailableException extends RuntimeException {
    public LeaderNotAvailableException() {
        super();
    }

    public LeaderNotAvailableException(String msg) {
        super(msg);
    }

    public LeaderNotAvailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
