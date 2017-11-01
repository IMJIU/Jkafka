package kafka.consumer;

/**
 * @author zhoulf
 * @create 2017-11-01 57 17
 **/

public class ConsumerThreadId extends Ordered<ConsumerThreadId> {
    public String consumer;
    public Integer threadId;

    @Override
    public String toString() {
        return String.format("%s-%d", consumer, threadId);
    }

    public void compare(ConsumerThreadId that){
        return toString().compare(that.toString());
    }
}
