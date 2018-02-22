package kafka.common;

public class OffsetsLoadInProgressException extends RuntimeException {
    public OffsetsLoadInProgressException() {
        super();
    }
    public OffsetsLoadInProgressException(String msg){
        super(msg);
    }
}
