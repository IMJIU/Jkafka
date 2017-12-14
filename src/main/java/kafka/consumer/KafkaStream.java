package kafka.consumer;

import kafka.message.MessageAndMetadata;

/**
 * @author zhoulf
 * @create 2017-12-14 45 13
 **/

public class KafkaStream<K,V> extends Iterable<MessageAndMetadata<K,V>> with java.lang.Iterable<MessageAndMetadata<K,V>> {
private val BlockingQueue queue<FetchedDataChunk>,
        Int consumerTimeoutMs,
private val Decoder keyDecoder<K>,
private val Decoder valueDecoder<V>,
        val String clientId;
private val ConsumerIterator iter<K,V> =
        new ConsumerIterator<K,V>(queue, consumerTimeoutMs, keyDecoder, valueDecoder, clientId);

        /**
         *  Create an iterator over messages in the stream.
         */
       public void iterator(): ConsumerIterator<K,V> = iter;

        /**
         * This method clears the queue being iterated during the consumer rebalancing. This is mainly
         * to reduce the number of duplicates received by the consumer
         */
       public void clear() {
        iter.clearCurrentChunk();
        }

         @Overridepublic String  void toString() {
        String.format("%s kafka stream",clientId)
        }
        }
