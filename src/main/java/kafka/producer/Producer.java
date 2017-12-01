package kafka.producer;

import kafka.metrics.KafkaMetricsGroup;
import kafka.producer.async.DefaultEventHandler;
import kafka.producer.async.EventHandler;
import kafka.utils.Logging;
import kafka.utils.Utils;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author zhoulf
 * @create 2017-11-30 42 14
 **/
// only for unit testing;
public class Producer<K,V>  extends Logging {
   public  ProducerConfig config;
    private EventHandler<K,V> eventHandler;

    public Producer(ProducerConfig config, EventHandler<K, V> eventHandler) {
        this.config = config;
        this.eventHandler = eventHandler;

        config.producerType match {
            case "sync" ->
            case "async" ->
                sync = false;
                producerSendThread = new ProducerSendThread<K,V>("ProducerSendThread-" + config.clientId,
                        queue,
                        eventHandler,
                        config.queueBufferingMaxMs,
                        config.batchNumMessages,
                        config.clientId);
                producerSendThread.start();
                KafkaMetricsReporter.startReporters(config.props);
                AppInfo.registerInfo();
        }
    }

    private AtomicBoolean hasShutdown = new AtomicBoolean(false);
private LinkedBlockingQueue queue = new LinkedBlockingQueue<KeyedMessage<K,V>>(config.queueBufferingMaxMessages);

private  Boolean sync = true;
private  ProducerSendThread<K,V> producerSendThread = null;
private Object lock = new Object();


private ProducerTopicStats producerTopicStats = ProducerTopicStatsRegistry.getProducerTopicStats(config.clientId);



       public  Producer(ProducerConfig config){
           this(config,
                   new DefaultEventHandler<K, V>(config,
                           Utils.createObject<Partitioner>(config.partitionerClass, config.props),
                   Utils.createObject<Encoder<V>>(config.serializerClass, config.props),
                   Utils.createObject<Encoder<K>>(config.keySerializerClass, config.props),
           new ProducerPool(config)));
    }


        /**
         * Sends the data, partitioned by key to the topic using either the
         * synchronous or the asynchronous producer
         * @param messages the producer data object that encapsulates the topic, key and message data
         */
       public void send(KeyedMessage messages<K,V>*) {
        lock synchronized {
        if (hasShutdown.get)
        throw new ProducerClosedException;
        recordStats(messages);
        sync match {
        case true -> eventHandler.handle(messages);
        case false -> asyncSend(messages);
        }
        }
        }

private  void recordStats(Seq messages<KeyedMessage<K,V>>) {
        for (message <- messages) {
        producerTopicStats.getProducerTopicStats(message.topic).messageRate.mark();
        producerTopicStats.getProducerAllTopicsStats.messageRate.mark();
        }
        }

private  void asyncSend(Seq messages<KeyedMessage<K,V>>) {
        for (message <- messages) {
        val added = config.queueEnqueueTimeoutMs match {
        case 0  ->
        queue.offer(message);
        case _  ->
        try {
        config.queueEnqueueTimeoutMs < 0 match {
        case true ->
        queue.put(message);
        true;
        case _ ->
        queue.offer(message, config.queueEnqueueTimeoutMs, TimeUnit.MILLISECONDS);
        }
        }
        catch {
        case InterruptedException e ->
        false;
        }
        }
        if(!added) {
        producerTopicStats.getProducerTopicStats(message.topic).droppedMessageRate.mark();
        producerTopicStats.getProducerAllTopicsStats.droppedMessageRate.mark();
        throw new QueueFullException("Event queue is full of unsent messages, could not send event: " + message.toString);
        }else {
        trace("Added to send queue an event: " + message.toString);
        trace("Remaining queue size: " + queue.remainingCapacity);
        }
        }
        }

        /**
         * Close API to close the producer pool connections to all Kafka brokers. Also closes
         * the zookeeper client connection if one exists
         */
       public void close()  {
        lock synchronized {
        val canShutdown = hasShutdown.compareAndSet(false, true);
        if(canShutdown) {
        info("Shutting down producer");
        val startTime = System.nanoTime();
        KafkaMetricsGroup.removeAllProducerMetrics(config.clientId);
        if (producerSendThread != null)
        producerSendThread.shutdown;
        eventHandler.close;
        info("Producer shutdown completed in " + (System.nanoTime() - startTime) / 1000000 + " ms");
        }
        }
        }
        }
