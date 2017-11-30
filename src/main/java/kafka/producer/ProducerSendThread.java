package kafka.producer;

/**
 * @author zhoulf
 * @create 2017-11-30 49 15
 **/

public class ProducerSendThread<K,V>(val String threadName,
        val BlockingQueue queue<KeyedMessage<K,V>>,
        val EventHandler handler<K,V>,
        val Long queueTime,
        val Int batchSize,
        val String clientId) extends Thread(threadName) with Logging with KafkaMetricsGroup {

private val shutdownLatch = new CountDownLatch(1);
private val shutdownCommand = new KeyedMessage<K,V]("shutdown", null.asInstanceOf[K], null.asInstanceOf[V>);

        newGauge("ProducerQueueSize",
        new Gauge<Integer> {
       public void value = queue.size;
        },
        Map("clientId" -> clientId));

         @Overridepublic void run {
        try {
        processEvents;
        }catch {
        case Throwable e -> error("Error in sending events: ", e);
        }finally {
        shutdownLatch.countDown;
        }
        }

       public void shutdown = {
        info("Begin shutting down ProducerSendThread");
        queue.put(shutdownCommand);
        shutdownLatch.await;
        info("Shutdown ProducerSendThread complete");
        }

privatepublic void processEvents() {
        var lastSend = SystemTime.milliseconds;
        var events = new ArrayBuffer<KeyedMessage<K,V>>
        var Boolean full = false;

        // drain the queue until you get a shutdown command;
        Stream.continually(queue.poll(scala.math.max(0, (lastSend + queueTime) - SystemTime.milliseconds), TimeUnit.MILLISECONDS));
        .takeWhile(item -> if(item != null) item ne shutdownCommand else true).foreach {
        currentQueueItem ->
        val elapsed = (SystemTime.milliseconds - lastSend);
        // check if the queue time is reached. This happens when the poll method above returns after a timeout and;
        // returns a null object;
        val expired = currentQueueItem == null;
        if(currentQueueItem != null) {
        trace("Dequeued item for topic %s, partition key: %s, data: %s";
        .format(currentQueueItem.topic, currentQueueItem.key, currentQueueItem.message))
        events += currentQueueItem;
        }

        // check if the batch size is reached;
        full = events.size >= batchSize;

        if(full || expired) {
        if(expired)
        debug(elapsed + " ms elapsed. Queue time reached. Sending..");
        if(full)
        debug("Batch full. Sending..");
        // if either queue time has reached or batch size has reached, dispatch to event handler;
        tryToHandle(events);
        lastSend = SystemTime.milliseconds;
        events = new ArrayBuffer<KeyedMessage<K,V>>
        }
        }
        // send the last batch of events;
        tryToHandle(events);
        if(queue.size > 0)
        throw new IllegalQueueStateException("Invalid queue state! After queue shutdown, %d remaining items in the queue";
        .format(queue.size))
        }

       public void tryToHandle(Seq events<KeyedMessage<K,V>>) {
        val size = events.size;
        try {
        debug("Handling " + size + " events");
        if(size > 0)
        handler.handle(events);
        }catch {
        case Throwable e -> error("Error in handling batch of " + size + " events", e);
        }
        }

        }
