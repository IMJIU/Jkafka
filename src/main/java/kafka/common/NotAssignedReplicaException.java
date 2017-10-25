package kafka.common;

public class NotAssignedReplicaException extends RuntimeException {
    public NotAssignedReplicaException(String msg){
        super(msg);
    }
}
