package kafka.producer.async;

/**
 * @author zhoulf
 * @create 2017-11-30 16:00
 **/

import kafka.producer.KeyedMessage;

import java.util.List;

/**
 * Handler that dispatches the batched data from the queue.
 */
public interface EventHandler<K, V> {

    /**
     * Callback to dispatch the batched data and send it to a Kafka server
     *
     * @param events the data sent to the producer
     */
    void handle(List<KeyedMessage<K, V>> events);

    /**
     * Cleans up and shuts down the event handler
     */
    void close();
}