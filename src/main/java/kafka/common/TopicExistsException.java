package kafka.common;

public class TopicExistsException extends RuntimeException {
    public TopicExistsException(String msg){
        super(msg);
    }
}
