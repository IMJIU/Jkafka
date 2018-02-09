package kafka.common;

public class OffsetMetadataTooLargeException extends RuntimeException {
    public OffsetMetadataTooLargeException(String msg){
        super(msg);
    }
}
