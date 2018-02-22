package kafka.common;

public class BrokerNotAvailableException extends RuntimeException {
    public BrokerNotAvailableException(String msg){
        super(msg);
    }
    public BrokerNotAvailableException() {
        super();
    }

}
