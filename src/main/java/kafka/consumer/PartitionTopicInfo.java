package kafka.consumer;

import kafka.message.ByteBufferMessageSet;
import kafka.utils.Logging;
import kafka.utils.Sc;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author zhoulf
 * @create 2017-10-13 02 18
 **/

public class PartitionTopicInfo extends Logging {
    public static final Long InvalidOffset = -1L;

    public static boolean isOffsetInvalid(Long offset) {
        return offset < 0L;
    }

    public String topic;
    public Integer partitionId;
    private BlockingQueue<FetchedDataChunk> chunkQueue;
    private AtomicLong consumedOffset;
    private AtomicLong fetchedOffset;
    private AtomicInteger fetchSize;
    private String clientId;

    public PartitionTopicInfo(java.lang.String topic, Integer partitionId, BlockingQueue<FetchedDataChunk> chunkQueue, AtomicLong consumedOffset, AtomicLong fetchedOffset, AtomicInteger fetchSize, java.lang.String clientId) {
        this.topic = topic;
        this.partitionId = partitionId;
        this.chunkQueue = chunkQueue;
        this.consumedOffset = consumedOffset;
        this.fetchedOffset = fetchedOffset;
        this.fetchSize = fetchSize;
        this.clientId = clientId;
        debug("initial consumer offset of " + this + " is " + consumedOffset.get());
        debug("initial fetch offset of " + this + " is " + fetchedOffset.get());
        consumerTopicStats = ConsumerTopicStatsRegistry.getConsumerTopicStat(clientId);
    }

    private ConsumerTopicStats consumerTopicStats;

    public Long getConsumeOffset() {
        return consumedOffset.get();
    }

    public Long getFetchOffset() {
        return fetchedOffset.get();
    }

    public void resetConsumeOffset(Long newConsumeOffset) {
        consumedOffset.set(newConsumeOffset);
        debug("reset consume offset of " + this + " to " + newConsumeOffset);
    }

    public void resetFetchOffset(Long newFetchOffset) {
        fetchedOffset.set(newFetchOffset);
        debug(String.format("reset fetch offset of ( %s ) to %d", this, newFetchOffset));
    }

    /**
     * Enqueue a message set for processing.
     */
    public void enqueue(ByteBufferMessageSet messages) {
        Integer size = messages.validBytes();
        try {
            if (size > 0) {
                Long next = Sc.last(messages.shallowIterator()).nextOffset();
                trace("Updating fetch offset = " + fetchedOffset.get() + " to " + next);

                chunkQueue.put(new FetchedDataChunk(messages, this, fetchedOffset.get()));

                fetchedOffset.set(next);
                debug(String.format("updated fetch offset of (%s) to %d", this, next));
                consumerTopicStats.getConsumerTopicStats(topic).byteRate.mark(size);
                consumerTopicStats.getConsumerAllTopicStats().byteRate.mark(size);
            } else if (messages.sizeInBytes() > 0) {
                chunkQueue.put(new FetchedDataChunk(messages, this, fetchedOffset.get()));
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return topic + ":" + partitionId.toString() + ": fetched offset = " + fetchedOffset.get() +
                ": consumed offset = " + consumedOffset.get();
    }

}
