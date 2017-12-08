package kafka.producer;

import kafka.common.AppInfo;
import kafka.common.ProducerClosedException;
import kafka.common.QueueFullException;
import kafka.metrics.KafkaMetricsGroup;
import kafka.producer.async.DefaultEventHandler;
import kafka.producer.async.EventHandler;
import kafka.utils.Logging;
import kafka.utils.Utils;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author zhoulf
 * @create 2017-11-30 42 14
 **/
// only for unit testing;
public class Producer<K, V> extends Logging {
    public ProducerConfig config;
    private EventHandler<K, V> eventHandler;
    private AtomicBoolean hasShutdown = new AtomicBoolean(false);
    private LinkedBlockingQueue queue = new LinkedBlockingQueue<KeyedMessage<K, V>>(config.queueBufferingMaxMessages());

    private Boolean sync = true;
    private ProducerSendThread<K, V> producerSendThread = null;
    private Object lock = new Object();

    private ProducerTopicStats producerTopicStats = ProducerTopicStatsRegistry.getProducerTopicStats(config.clientId());

    public Producer(ProducerConfig config, EventHandler<K, V> eventHandler) {
        this.config = config;
        this.eventHandler = eventHandler;

        if ("async".equals(config.producerType)) {
//            case "sync" ->
            sync = false;
            producerSendThread = new ProducerSendThread("ProducerSendThread-" + config.clientId(),
                    queue,
                    eventHandler,
                    config.queueBufferingMaxMs().longValue(),
                    config.batchNumMessages(),
                    config.clientId());
            producerSendThread.start();
            KafkaMetricsReporter.startReporters(config.props);
            AppInfo.registerInfo();
        }
    }


    public Producer(ProducerConfig config) {
        this(config,
                new DefaultEventHandler(config,
                        Utils.createObject(config.partitionerClass, config.props),
                        Utils.createObject(config.serializerClass(), config.props),
                        Utils.createObject(config.keySerializerClass(), config.props),
                        new ProducerPool(config), null));
    }


    /**
     * Sends the data, partitioned by key to the topic using either the
     * synchronous or the asynchronous producer
     *
     * @param messages the producer data object that encapsulates the topic, key and message data
     */
    public void send(List<KeyedMessage<K, V>> messages) {
        synchronized (lock) {
            if (hasShutdown.get())
                throw new ProducerClosedException();
            recordStats(messages);
            if (sync)
                eventHandler.handle(messages);
            else
                asyncSend(messages);
        }
    }

    private void recordStats(List<KeyedMessage<K, V>> messages) {
        for (KeyedMessage<K, V> message : messages) {
            producerTopicStats.getProducerTopicStats(message.topic).messageRate.mark();
            producerTopicStats.getProducerAllTopicsStats().messageRate.mark();
        }
    }

    private void asyncSend(List<KeyedMessage<K, V>> messages) {
        for (KeyedMessage<K, V> message : messages) {
            boolean added = false;
            if (config.queueEnqueueTimeoutMs() == 0) {
                queue.offer(message);
            } else {
                try {
                    if (config.queueEnqueueTimeoutMs() < 0) {
                        queue.put(message);
                        added = true;
                    } else {
                        queue.offer(message, config.queueEnqueueTimeoutMs(), TimeUnit.MILLISECONDS);
                    }
                } catch (InterruptedException e) {
                    added = false;
                }
            }
            if (!added) {
                producerTopicStats.getProducerTopicStats(message.topic).droppedMessageRate.mark();
                producerTopicStats.getProducerAllTopicsStats().droppedMessageRate.mark();
                throw new QueueFullException("Event queue is full of unsent messages, could not send event: " + message.toString());
            } else {
                trace("Added to send queue an event: " + message.toString());
                trace("Remaining queue size: " + queue.remainingCapacity());
            }
        }
    }

    /**
     * Close API to close the producer pool connections to all Kafka brokers. Also closes
     * the zookeeper client connection if one exists
     */
    public void close() {
        synchronized (lock) {
            boolean canShutdown = hasShutdown.compareAndSet(false, true);
            if (canShutdown) {
                info("Shutting down producer");
                Long startTime = System.nanoTime();
                KafkaMetricsGroup.removeAllProducerMetrics(config.clientId());
                if (producerSendThread != null)
                    producerSendThread.shutdown();
                eventHandler.close();
                info("Producer shutdown completed in " + (System.nanoTime() - startTime) / 1000000 + " ms");
            }
        }
    }
}
