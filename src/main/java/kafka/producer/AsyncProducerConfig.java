package kafka.producer;

import kafka.utils.VerifiableProperties;

/**
 * @author zhoulf
 * @create 2017-12-01 15:05
 **/
public interface AsyncProducerConfig {
    VerifiableProperties props();

    /* maximum time, in milliseconds, for buffering data on the producer queue */
    default Integer queueBufferingMaxMs(){
        return props().getInt("queue.buffering.max.ms", 5000);
    }

    /** the maximum size of the blocking queue for buffering on the producer */
    default int queueBufferingMaxMessages(){
        return props().getInt("queue.buffering.max.messages", 10000);
    }

    /**
     * Timeout for event enqueue:
     * 0: events will be enqueued immediately or dropped if the queue is full
     * -ve: enqueue will block indefinitely if the queue is full
     * +ve: enqueue will block up to this many milliseconds if the queue is full
     */
    default int queueEnqueueTimeoutMs(){
        return props().getInt("queue.enqueue.timeout.ms", -1);
    }

    /** the number of messages batched at the producer */
    default int  batchNumMessages (){
        return props().getInt("batch.num.messages", 200);
    }

    /** the serializer class for defaultues */
    default String serializerClass(){
        return props().getString("serializer.class", "kafka.serializer.DefaultEncoder");
    }

    /** the serializer class for keys (defaults to the same as for values) */
    default String keySerializerClass (){
        return props().getString("key.serializer.class", serializerClass());
    }
}
