package kafka.consumer;

/**
 * @author zhoulf
 * @create 2017-11-01 57 17
 **/

public class ConsumerThreadId implements Comparable<ConsumerThreadId> {
    public String consumer;
    public Integer threadId;

    public ConsumerThreadId(String consumer, Integer threadId) {
        this.consumer = consumer;
        this.threadId = threadId;
    }

    @Override
    public String toString() {
        return String.format("%s-%d", consumer, threadId);
    }


    @Override
    public int compareTo(ConsumerThreadId that) {
        return toString().compareTo(that.toString());
    }
}
