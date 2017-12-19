package kafka.javaapi.producer;

/**
 * @author zhoulf
 * @create 2017-12-19 14 20
 **/

class Producer<K,V>(private val kafka underlying.producer.Producer<K,V>) // for testing only;
        {
       public void this(ProducerConfig config) = this(new kafka.producer.Producer<K,V>(config));
        /**
         * Sends the data to a single topic, partitioned by key, using either the
         * synchronous or the asynchronous producer
         * @param message the producer data object that encapsulates the topic, key and message data
         */
       public void send(KeyedMessage message<K,V>) {
        underlying.send(message);
        }

        /**
         * Use this API to send data to multiple topics
         * @param messages list of producer data objects that encapsulate the topic, key and message data
         */
       public void send(java messages.util.List<KeyedMessage<K,V>>) {
        import collection.JavaConversions._;
        underlying.send((mutable messages.Buffer<KeyedMessage<K,V>>)._ toSeq*);
        }

        /**
         * Close API to close the producer pool connections to all Kafka brokers. Also closes
         * the zookeeper client connection if one exists
         */
       public void close = underlying.close;
        }
