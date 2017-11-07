package kafka.common;

public class LeaderElectionNotNeededException extends RuntimeException {
    public LeaderElectionNotNeededException(String msg){
        super(msg);
    }
}
