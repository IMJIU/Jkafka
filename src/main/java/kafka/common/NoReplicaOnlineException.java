package kafka.common;

public class NoReplicaOnlineException extends RuntimeException {
    public NoReplicaOnlineException(String msg){
        super(msg);
    }
}
