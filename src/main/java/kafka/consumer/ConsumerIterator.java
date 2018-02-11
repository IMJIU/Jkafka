package kafka.consumer;

/**
 * @author zhoulf
 * @create 2017-12-14 06 14
 **/

import kafka.common.ConsumerTimeoutException;
import kafka.common.KafkaException;
import kafka.common.MessageSizeTooLargeException;
import kafka.message.MessageAndMetadata;
import kafka.message.MessageAndOffset;
import kafka.serializer.Decoder;
import kafka.utils.IteratorTemplate;
import kafka.utils.Logging;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An iterator that blocks until a value can be read from the supplied queue.
 * The iterator takes a shutdownCommand object which can be added to the queue to trigger a shutdown
 */
public class ConsumerIterator<K, V> extends IteratorTemplate<MessageAndMetadata<K, V>> {
    private Logging log = Logging.getLogger(this.getClass().getName());
    private BlockingQueue<FetchedDataChunk> channel;
    public Integer consumerTimeoutMs;
    private Decoder<K> keyDecoder;
    private Decoder<V> valueDecoder;
    public String clientId;

    public ConsumerIterator(BlockingQueue<FetchedDataChunk> channel, Integer consumerTimeoutMs, Decoder<K> keyDecoder, Decoder<V> valueDecoder, String clientId) {
        this.channel = channel;
        this.consumerTimeoutMs = consumerTimeoutMs;
        this.keyDecoder = keyDecoder;
        this.valueDecoder = valueDecoder;
        this.clientId = clientId;
    }

    private AtomicReference<Iterator<MessageAndOffset>> current = new AtomicReference(null);

    private PartitionTopicInfo currentTopicInfo = null;
    private Long consumedOffset = -1L;
    private ConsumerTopicStats consumerTopicStats = ConsumerTopicStatsRegistry.getConsumerTopicStat(clientId);

    @Override
    public MessageAndMetadata<K, V> next() {
        MessageAndMetadata<K, V> item = super.next();
        if (consumedOffset < 0)
            throw new KafkaException(String.format("Offset returned by the message set is invalid %d", consumedOffset));
        currentTopicInfo.resetConsumeOffset(consumedOffset);
        String topic = currentTopicInfo.topic;
        log.trace(String.format("Setting %s consumed offset to %d", topic, consumedOffset));
        consumerTopicStats.getConsumerTopicStats(topic).messageRate.mark();
        consumerTopicStats.getConsumerAllTopicStats().messageRate.mark();
        return item;
    }

    protected MessageAndMetadata<K, V> makeNext() {
        FetchedDataChunk currentDataChunk = null;
        // if we don't have an iterator, get one;
        Iterator<MessageAndOffset> localCurrent = current.get();
        if (localCurrent == null || !localCurrent.hasNext()) {
            try {
                if (consumerTimeoutMs < 0)
                    currentDataChunk = channel.take();
                else {
                    currentDataChunk = channel.poll(consumerTimeoutMs, TimeUnit.MILLISECONDS);
                    if (currentDataChunk == null) {
                        // reset state to make the iterator re-iterable;
                        resetState();
                        throw new ConsumerTimeoutException();
                    }
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            if (currentDataChunk.equals(ZookeeperConsumerConnector.shutdownCommand)) {
                log.debug("Received the shutdown command");
                return allDone();
            } else {
                currentTopicInfo = currentDataChunk.topicInfo;
                Long cdcFetchOffset = currentDataChunk.fetchOffset;
                Long ctiConsumeOffset = currentTopicInfo.getConsumeOffset();
                if (ctiConsumeOffset < cdcFetchOffset) {
                    log.error(String.format("consumed offset: %d doesn't match fetch offset: %d for %s;\n Consumer may lose data",
                            ctiConsumeOffset, cdcFetchOffset, currentTopicInfo));
                    currentTopicInfo.resetConsumeOffset(cdcFetchOffset);
                }
                localCurrent = currentDataChunk.messages.iterator();

                current.set(localCurrent);
            }
            // if we just updated the current chunk and it is empty that means the fetch size is too small!;
            if (currentDataChunk.messages.validBytes() == 0)
                throw new MessageSizeTooLargeException(String.format("Found a message larger than the maximum fetch size of this consumer on topic " +
                                "%s partition %d at fetch offset %d. Increase the fetch size, or decrease the maximum message size the broker will allow.",
                        currentDataChunk.topicInfo.topic, currentDataChunk.topicInfo.partitionId, currentDataChunk.fetchOffset));
        }
        MessageAndOffset item = localCurrent.next();
        // reject the messages that have already been consumed;
        while (item.offset < currentTopicInfo.getConsumeOffset() && localCurrent.hasNext()) {
            item = localCurrent.next();
        }
        consumedOffset = item.nextOffset();

        item.message.ensureValid(); // validate checksum of message to ensure it is valid;

        return new MessageAndMetadata(currentTopicInfo.topic, currentTopicInfo.partitionId, item.message, item.offset, keyDecoder, valueDecoder);
    }

    public void clearCurrentChunk() {
        log.debug("Clearing the current data chunk for this consumer iterator");
        current.set(null);
    }
}
