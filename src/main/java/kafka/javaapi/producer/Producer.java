package kafka.javaapi.producer;

/**
 * @author zhoulf
 * @create 2017-12-19 20:14
 **/

class Producer[K,V](private val underlying: kafka.producer.Producer[K,V]) // for testing only
        {
        def this(config: ProducerConfig) = this(new kafka.producer.Producer[K,V](config))
        /**
         * Sends the data to a single topic, partitioned by key, using either the
         * synchronous or the asynchronous producer
         * @param message the producer data object that encapsulates the topic, key and message data
         */
        def send(message: KeyedMessage[K,V]) {
        underlying.send(message)
        }

        /**
         * Use this API to send data to multiple topics
         * @param messages list of producer data objects that encapsulate the topic, key and message data
         */
        def send(messages: java.util.List[KeyedMessage[K,V]]) {
        import collection.JavaConversions._
        underlying.send((messages: mutable.Buffer[KeyedMessage[K,V]]).toSeq: _*)
        }

        /**
         * Close API to close the producer pool connections to all Kafka brokers. Also closes
         * the zookeeper client connection if one exists
         */
        def close = underlying.close
        }