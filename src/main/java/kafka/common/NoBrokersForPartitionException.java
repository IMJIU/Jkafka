package kafka.common;

public class NoBrokersForPartitionException extends RuntimeException {
    public NoBrokersForPartitionException(String msg){
        super(msg);
    }
}
