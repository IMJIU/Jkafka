package kafka.server;


import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import kafka.api.FetchRequest;
import kafka.api.FetchResponse;
import kafka.api.FetchResponsePartitionData;
import kafka.cluster.Broker;
import kafka.common.ClientIdAndBroker;
import kafka.common.ErrorMapping;
import kafka.common.KafkaException;
import kafka.consumer.PartitionTopicInfo;
import kafka.consumer.SimpleConsumer;
import kafka.func.Handler;
import kafka.log.TopicAndPartition;
import kafka.message.ByteBufferMessageSet;
import kafka.message.InvalidMessageException;
import kafka.message.MessageAndOffset;
import kafka.metrics.KafkaMetricsGroup;
import kafka.utils.Pool;
import kafka.utils.ShutdownableThread;
import kafka.utils.Utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Abstract class for fetching data from multiple partitions from the same broker.
 */
public abstract class AbstractFetcherThread extends ShutdownableThread {
    public String name;
    public String clientId;
    public Broker sourceBroker;
    public Integer socketTimeout;
    public Integer socketBufferSize;
    public Integer fetchSize;
    public Integer fetcherBrokerId = -1;
    public Integer maxWait = 0;
    public Integer minBytes = 1;

    public AbstractFetcherThread(String name, Boolean isInterruptible, String clientId, Broker sourceBroker, Integer socketTimeout, Integer socketBufferSize, Integer fetchSize, Integer fetcherBrokerId, Integer maxWait, Integer minBytes) {
        super(name, isInterruptible);
        this.name = name;
        this.clientId = clientId;
        this.sourceBroker = sourceBroker;
        this.socketTimeout = socketTimeout;
        this.socketBufferSize = socketBufferSize;
        this.fetchSize = fetchSize;
        this.fetcherBrokerId = fetcherBrokerId;
        this.maxWait = maxWait;
        this.minBytes = minBytes;
        simpleConsumer = new SimpleConsumer(sourceBroker.host, sourceBroker.port, socketTimeout, socketBufferSize, clientId);
        metricId = new ClientIdAndBroker(clientId, sourceBroker.host, sourceBroker.port);
        fetcherStats = new FetcherStats(metricId);
        fetcherLagStats = new FetcherLagStats(metricId);
        fetchRequestBuilder = new FetchRequestBuilder().
                clientId(clientId).
                replicaId(fetcherBrokerId).
                maxWait(maxWait).
                minBytes(minBytes);
    }

    Boolean isInterruptible = true;
    private Map<TopicAndPartition, Long> partitionMap = Maps.newHashMap(); // a (topic, partition) -> offset map;
    private ReentrantLock partitionMapLock = new ReentrantLock();
    private Condition partitionMapCond = partitionMapLock.newCondition();
    public SimpleConsumer simpleConsumer;
    private ClientIdAndBroker metricId;
    FetcherStats fetcherStats;
    FetcherLagStats fetcherLagStats;
    FetchRequestBuilder fetchRequestBuilder;

  /* callbacks to be defined in subclass */

    // process fetched data;
    public abstract void processPartitionData(TopicAndPartition topicAndPartition, Long fetchOffset, FetchResponsePartitionData partitionData);

    // handle a partition whose offset is out of range and return a new fetch offset;
    public abstract Long handleOffsetOutOfRange(TopicAndPartition topicAndPartition) throws Throwable;

    // deal with partitions with errors, potentially due to leadership changes;
    public abstract void handlePartitionsWithErrors(Iterable<TopicAndPartition> partitions);

    @Override
    public void shutdown() {
        super.shutdown();
        simpleConsumer.close();
    }

    @Override
    public void doWork() {
        Utils.inLock(partitionMapLock, () -> {
            if (partitionMap.isEmpty()) {
                try {
                    partitionMapCond.await(200L, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            partitionMap.forEach((topicAndPartition, offset) ->
                    fetchRequestBuilder.addFetch(topicAndPartition.topic, topicAndPartition.partition, offset, fetchSize));
        });

        FetchRequest fetchRequest = fetchRequestBuilder.build();
        if (!fetchRequest.requestInfo.isEmpty())
            processFetchRequest(fetchRequest);
    }

    private void processFetchRequest(FetchRequest fetchRequest) {
        final Set<TopicAndPartition> partitionsWithError = Sets.newHashSet();
        FetchResponse response = null;
        try {
            logger.trace(String.format("Issuing to broker %d of fetch request %s", sourceBroker.id, fetchRequest));
            response = simpleConsumer.fetch(fetchRequest);
        } catch (Throwable t) {
            if (isRunning.get()) {
                logger.warn(String.format("Error in fetch %s. Possible cause: %s", fetchRequest, t.toString()));
                synchronized (partitionMapLock) {
                    partitionsWithError.addAll(partitionMap.keySet());
                }
            }
        }
        fetcherStats.requestRate.mark();
        if (response != null) {
            final FetchResponse response2 = response;
            // process fetched data;
            Utils.inLock(partitionMapLock, () ->
                    response2.data.forEach((topicAndPartition, partitionData) -> {
                        String topic = topicAndPartition.topic;
                        Integer partitionId = topicAndPartition.partition;
                        Long currentOffset = partitionMap.get(topicAndPartition);
                        // we append to the log if the current offset is defined and it is the same as the offset requested during fetch;
                        if (currentOffset != null && fetchRequest.requestInfo.get(topicAndPartition).offset == currentOffset) {
                            if (partitionData.error == ErrorMapping.NoError) {
                                try {
                                    ByteBufferMessageSet messages = (ByteBufferMessageSet) partitionData.messages;
                                    Integer validBytes = messages.validBytes();
                                    Optional<MessageAndOffset> lastOption = Utils.lastOption(messages.shallowIterator());
                                    Long newOffset;
                                    if (lastOption.isPresent()) {
                                        newOffset = lastOption.get().nextOffset();
                                    } else {
                                        newOffset = currentOffset;
                                    }
                                    partitionMap.put(topicAndPartition, newOffset);
                                    fetcherLagStats.getFetcherLagStats(topic, partitionId).lag(partitionData.hw - newOffset);
                                    fetcherStats.byteRate.mark(validBytes);
                                    // Once we hand off the partition data to the subclass, we can't mess with it any more in this thread;
                                    processPartitionData(topicAndPartition, currentOffset, partitionData);
                                } catch (InvalidMessageException ime) {
                                    // we log the error and continue. This ensures two things;
                                    // 1. If there is a corrupt message in a topic partition, it does not bring the fetcher thread down and cause other topic partition to also lag;
                                    // 2. If the message is corrupt due to a transient state in the log (truncation, partial writes can cause this), we simply continue and;
                                    //    should get fixed in the subsequent fetches;
                                    logger.error("Found invalid messages during fetch for partition <" + topic + "," + partitionId + "> offset " + currentOffset + " error " + ime.getMessage());
                                } catch (Throwable e2) {
                                    throw new KafkaException(String.format("error processing data for partition <%s,%d> offset %d", topic, partitionId, currentOffset), e2);
                                }
                            } else if (partitionData.error == ErrorMapping.OffsetOutOfRangeCode) {
                                try {
                                    Long newOffset = handleOffsetOutOfRange(topicAndPartition);
                                    partitionMap.put(topicAndPartition, newOffset);
                                    logger.error(String.format("Current offset %d for partition <%s,%d> out of range; reset offset to %d", currentOffset, topic, partitionId, newOffset));
                                } catch (Throwable e2) {
                                    logger.error(String.format("Error getting offset for partition <%s,%d> to broker %d", topic, partitionId, sourceBroker.id), e2);
                                    partitionsWithError.add(topicAndPartition);
                                }
                            } else {
                                if (isRunning.get()) {
                                    logger.error(String.format("Error for partition <%s,%d> to broker %d:%s", topic, partitionId, sourceBroker.id,
                                            ErrorMapping.exceptionFor(partitionData.error).getClass()));
                                    partitionsWithError.add(topicAndPartition);
                                }
                            }
                        }
                    })
            );
        }
        if (partitionsWithError.size() > 0) {
            logger.debug(String.format("handling partitions with error for %s", partitionsWithError));
            handlePartitionsWithErrors(partitionsWithError);
        }
    }

    public void addPartitions(Map<TopicAndPartition, Long> partitionAndOffsets) {
        try {
            partitionMapLock.lockInterruptibly();
            for (TopicAndPartition topicAndPartition : partitionAndOffsets.keySet()) {
                Long offset = partitionAndOffsets.get(topicAndPartition);
                // If the partitionMap already has the topic/partition, then do not update the map with the old offset;
                if (!partitionMap.containsKey(topicAndPartition))
                    partitionMap.put(topicAndPartition, PartitionTopicInfo.isOffsetInvalid(offset) ? handleOffsetOutOfRange(topicAndPartition) : offset);
            }
            partitionMapCond.signalAll();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        } finally {
            partitionMapLock.unlock();
        }
    }

    public void removePartitions(Set<TopicAndPartition> topicAndPartitions) {
        try {
            partitionMapLock.lockInterruptibly();
            topicAndPartitions.forEach(tp -> partitionMap.remove(tp));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            partitionMapLock.unlock();
        }
    }

    public Integer partitionCount() {
        try {
            partitionMapLock.lockInterruptibly();
            return partitionMap.size();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            partitionMapLock.unlock();
        }
    }
}

class FetcherLagMetrics extends KafkaMetricsGroup {
    public ClientIdTopicPartition metricId;

    public FetcherLagMetrics(ClientIdTopicPartition metricId) {
        this.metricId = metricId;
        newGauge("ConsumerLag", new Gauge<Long>() {
            @Override
            public Long value() {
                return lagVal.get();
            }
        }, ImmutableMap.of("clientId", metricId.clientId,
                "topic", metricId.topic,
                "partition", metricId.partitionId.toString()));


    }

    private AtomicLong lagVal = new AtomicLong(-1L);


    public void lag(Long newLag) {
        lagVal.set(newLag);
    }

    public Long lag() {
        return lagVal.get();
    }
}

class FetcherLagStats {
    public ClientIdAndBroker metricId;

    public FetcherLagStats(ClientIdAndBroker metricId) {
        this.metricId = metricId;
    }

    private Handler<ClientIdTopicPartition, FetcherLagMetrics> valueFactory = (k) -> new FetcherLagMetrics(k);
    Pool<ClientIdTopicPartition, FetcherLagMetrics> stats = new Pool<>(Optional.of(valueFactory));

    public FetcherLagMetrics getFetcherLagStats(String topic, Integer partitionId) {
        return stats.getAndMaybePut(new ClientIdTopicPartition(metricId.clientId, topic, partitionId));
    }
}

class FetcherStats extends KafkaMetricsGroup {
    ClientIdAndBroker metricId;

    public FetcherStats(ClientIdAndBroker metricId) {
        this.metricId = metricId;
        tags = new HashMap() {{
            put("clientId", metricId.clientId);
            put("brokerHost", metricId.brokerHost);
            put("brokerPort", metricId.brokerPort.toString());
        }};
        requestRate = newMeter("RequestsPerSec", "requests", TimeUnit.SECONDS, tags);
        byteRate = newMeter("BytesPerSec", "bytes", TimeUnit.SECONDS, tags);
    }

    Map tags;

    Meter requestRate;

    Meter byteRate;
}

