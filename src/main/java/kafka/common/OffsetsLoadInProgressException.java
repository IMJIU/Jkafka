package kafka.common;

public class OffsetsLoadInProgressException extends RuntimeException {
    public OffsetsLoadInProgressException(String msg){
        super(msg);
    }
}
