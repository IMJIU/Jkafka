package kafka.producer;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.yammer.metrics.core.Gauge;
import kafka.common.IllegalQueueStateException;
import kafka.func.Action;
import kafka.metrics.KafkaMetricsGroup;
import kafka.producer.async.EventHandler;
import kafka.utils.Logging;
import kafka.utils.Time;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author zhoulf
 * @create 2017-11-30 49 15
 **/

//with Logging with KafkaMetricsGroup
public class ProducerSendThread<K, V> extends Thread {
    private static final Logging log = Logging.getLogger(ProducerSendThread.class.getName());
    public KafkaMetricsGroup kafkaMetricsGroup = new KafkaMetricsGroup();
    public String threadName;
    public BlockingQueue<KeyedMessage<K, V>> queue;
    public EventHandler<K, V> handler;
    public Long queueTime;
    public Integer batchSize;
    public String clientId;

    private CountDownLatch shutdownLatch = new CountDownLatch(1);
    private KeyedMessage shutdownCommand = new KeyedMessage<K, V>("shutdown", null, null);

    public ProducerSendThread(String threadName) {
        super(threadName);
        this.threadName = threadName;
        kafkaMetricsGroup.newGauge("ProducerQueueSize", new Gauge<Integer>() {
                    public Integer value() {
                        return queue.size();
                    }
                },
                ImmutableMap.of("clientId", clientId));
    }


    @Override
    public void run() {
        try {
            processEvents.invoke();
        } catch (Throwable e) {
            log.error("Error in sending events: ", e);
        } finally {
            shutdownLatch.countDown();
        }
    }

    public void shutdown() {
        log.info("Begin shutting down ProducerSendThread");
        try {
            queue.put(shutdownCommand);
            shutdownLatch.await();
            log.info("Shutdown ProducerSendThread complete");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    private Action processEvents = () -> {
        Long lastSend = Time.get().milliseconds();
        List<KeyedMessage<K, V>> events = Lists.newArrayList();
        Boolean full = false;
        try {
            KeyedMessage<K, V> currentQueueItem = null;
            // drain the queue until you get a shutdown command;
            do {
                currentQueueItem = queue.poll(Math.max(0, (lastSend + queueTime) - Time.get().milliseconds()), TimeUnit.MILLISECONDS);
                Long elapsed = (Time.get().milliseconds() - lastSend);
                // check if the queue time is reached. This happens when the poll method above returns after a timeout and;
                // returns a null object;
                boolean expired = currentQueueItem == null;
                if (currentQueueItem != null) {
                    log.trace(String.format("Dequeued item for topic %s, partition key: %s, data: %s",
                            currentQueueItem.topic, currentQueueItem.key, currentQueueItem.message));
                    events.add(currentQueueItem);
                }

                // check if the batch size is reached;
                full = events.size() >= batchSize;

                if (full || expired) {
                    if (expired)
                        log.debug(elapsed + " ms elapsed. Queue time reached. Sending..");
                    if (full)
                        log.debug("Batch full. Sending..");
                    // if either queue time has reached or batch size has reached, dispatch to event handler;
                    tryToHandle(events);
                    lastSend = Time.get().milliseconds();
                    events = Lists.newArrayList();
                }
            } while (currentQueueItem == null || !currentQueueItem.equals(shutdownCommand));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        tryToHandle(events);
        if (queue.size() > 0)
            throw new IllegalQueueStateException(String.format("Invalid queue state! After queue shutdown, %d remaining items in the queue", queue.size()));
    };

    public void tryToHandle(List<KeyedMessage<K, V>> events) {
        int size = events.size();
        try {
            log.debug("Handling " + size + " events");
            if (size > 0)
                handler.handle(events);
        } catch (Throwable e) {
            log.error("Error in handling batch of " + size + " events", e);
        }
    }

}
