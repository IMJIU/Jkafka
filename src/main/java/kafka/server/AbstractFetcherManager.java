package kafka.server;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.yammer.metrics.core.Gauge;
import kafka.cluster.Broker;
import kafka.func.IntCount;
import kafka.func.NumCount;
import kafka.log.TopicAndPartition;
import kafka.metrics.KafkaMetricsGroup;
import kafka.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author zhoulf
 * @create 2017-10-13 50 10
 **/

public abstract class AbstractFetcherManager extends KafkaMetricsGroup {
    protected String name;
    public String clientId;
    public Integer numFetchers = 1;
    // map of (source broker_id, fetcher_id per source broker) => fetcher;
    private Map<BrokerAndFetcherId, AbstractFetcherThread> fetcherThreadMap = Maps.newHashMap();
    private Object mapLock = new Object();

    public AbstractFetcherManager(String name, String clientId, Integer numFetchers) {
        this.name = name;
        this.clientId = clientId;
        this.numFetchers = numFetchers;
        this.logIdent = "<" + name + "> ";
        newGauge("MaxLag", new Gauge<Object>() {
            // current max lag across all fetchers/topics/partitions;
            public Object value() {
                NumCount<Long> curMaxAll = NumCount.of(0L);
                fetcherThreadMap.entrySet().forEach(en -> {
                    NumCount<Long> curMaxThread = NumCount.of(0);
                    en.getValue().fetcherLagStats.stats.values().forEach(en2 -> {
                        curMaxThread.set(Math.max(curMaxThread.get(), en2.lag()));
                        curMaxThread.set(Math.max(curMaxAll.get(), curMaxThread.get()));
                    });
                });
                return curMaxAll.get();
            }
        }, ImmutableMap.of("clientId", clientId));

        newGauge("MinFetchRate", new Gauge<Double>() {
                    // current min fetch rate across all fetchers/topics/partitions;
                    public Double value() {
                        Double headRate = fetcherThreadMap.values().iterator().next().fetcherStats.requestRate.oneMinuteRate();
                        NumCount<Double> curMinAll = NumCount.of(headRate);
                        fetcherThreadMap.values().forEach(fetcherThreadMapEntry ->
                                curMinAll.set(Math.min(curMinAll.get(), fetcherThreadMapEntry.fetcherStats.requestRate.oneMinuteRate())));
                        return curMinAll.get();
                    }
                },
                ImmutableMap.of("clientId", clientId));
    }

    private Integer getFetcherId(String topic, Integer partitionId) {
        return Utils.abs(31 * topic.hashCode() + partitionId) % numFetchers;
    }

    // to be defined in subclass to create a specific fetcher;
    public abstract AbstractFetcherThread createFetcherThread(Integer fetcherId, Broker sourceBroker);

    public void addFetcherForPartitions(Map<TopicAndPartition, BrokerAndInitialOffset> partitionAndOffsets) {
        synchronized (mapLock) {
            Map<BrokerAndFetcherId,List<BrokerAndInitialOffset>>partitionsPerFetcher= Utils.groupBy(partitionAndOffsets.entrySet(), en->{
                BrokerAndInitialOffset brokerAndInitialOffset = en.getValue();
                TopicAndPartition topicAndPartition = en.getKey();
                return new BrokerAndFetcherId(brokerAndInitialOffset.broker, getFetcherId(topicAndPartition.topic, topicAndPartition.partition));
            });
            for(Map.Entry<BrokerAndFetcherId,List<BrokerAndInitialOffset>> en :partitionsPerFetcher.entrySet()){
                BrokerAndFetcherId brokerAndFetcherId = en.getKey();
                List<BrokerAndInitialOffset> partitionAndOffsetList = en.getValue();

                 AbstractFetcherThread fetcherThread = null;
                AbstractFetcherThread f = fetcherThreadMap.get(brokerAndFetcherId);
                if(f!=null) {
                    fetcherThread = f;
                }else{
                        fetcherThread = createFetcherThread(brokerAndFetcherId.fetcherId, brokerAndFetcherId.broker);
                        fetcherThreadMap.put(brokerAndFetcherId, fetcherThread);
                        fetcherThread.start();
                }

                fetcherThreadMap.get(brokerAndFetcherId).addPartitions(Utils.mapValue(partitionAndOffsets,brokerAndInitOffset->brokerAndInitOffset.initOffset));
            }
        }

        info(String.format("Added fetcher for partitions %s", Utils.map(partitionAndOffsets,en->{
            TopicAndPartition topicAndPartition = en.getKey();
            BrokerAndInitialOffset brokerAndInitialOffset = en.getValue();
                return "<" + topicAndPartition + ", initOffset " + brokerAndInitialOffset.initOffset + " to broker " + brokerAndInitialOffset.broker + "> ";
        })));
    }

    public void removeFetcherForPartitions(Set<TopicAndPartition> partitions) {
        synchronized (mapLock) {
            for (AbstractFetcherThread fetcher : fetcherThreadMap.values()) {
                fetcher.removePartitions(partitions);
            }
        }
        info(String.format("Removed fetcher for partitions %s", partitions));
    }

    public void shutdownIdleFetcherThreads() {
        synchronized (mapLock) {
            Set<BrokerAndFetcherId> keysToBeRemoved = Sets.newHashSet();
            for (BrokerAndFetcherId key : fetcherThreadMap.keySet()) {
                AbstractFetcherThread fetcher = fetcherThreadMap.get(key);
                if (fetcher.partitionCount() <= 0) {
                    fetcher.shutdown();
                    keysToBeRemoved.add(key);
                }
            }
            fetcherThreadMap.remove(keysToBeRemoved);
        }
    }

    public void closeAllFetchers() {
        synchronized (mapLock) {
            for (AbstractFetcherThread fetcher : fetcherThreadMap.values()) {
                fetcher.shutdown();
            }
            fetcherThreadMap.clear();
        }
    }
}

class BrokerAndFetcherId {
    public Broker broker;
    public Integer fetcherId;

    public BrokerAndFetcherId(Broker broker, Integer fetcherId) {
        this.broker = broker;
        this.fetcherId = fetcherId;
    }
}

class BrokerAndInitialOffset {
    Broker broker;
    Long initOffset;

    public BrokerAndInitialOffset(Broker broker, Long initOffset) {
        this.broker = broker;
        this.initOffset = initOffset;
    }
}
