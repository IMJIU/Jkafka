package kafka.server;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.yammer.metrics.core.Gauge;
import kafka.metrics.KafkaMetricsGroup;

import java.util.Map;

/**
 * @author zhoulf
 * @create 2017-10-13 50 10
 **/

public abstract class AbstractFetcherManager extends KafkaMetricsGroup {
protected  String name;
public String clientId;
public Integer numFetchers = 1;

    public AbstractFetcherManager(String name, String clientId, java.lang.Integer numFetchers) {
        this.name = name;
        this.clientId = clientId;
        this.numFetchers = numFetchers;
        this.logIdent = "<" + name + "> ";
        newGauge("MaxLag", new Gauge<Object>() {
            // current max lag across all fetchers/topics/partitions;
            public Object value(){
                fetcherThreadMap.foldLeft(0L)
            ((curMaxAll, fetcherThreadMapEntry) => {
                fetcherThreadMapEntry._2.fetcherLagStats.stats.foldLeft(0L)((curMaxThread, fetcherLagStatsEntry) => {
                    curMaxThread.max(fetcherLagStatsEntry._2.lag);
                }).max(curMaxAll);
            });
        },
        ImmutableMap.of("clientId" , clientId);
        );

        newGauge(
                "MinFetchRate", {
                        new Gauge<Double> {
                // current min fetch rate across all fetchers/topics/partitions;
        public void value = {
                val Double headRate =
                fetcherThreadMap.headOption.map(_._2.fetcherStats.requestRate.oneMinuteRate).getOrElse(0);

        fetcherThreadMap.foldLeft(headRate)((curMinAll, fetcherThreadMapEntry) => {
            fetcherThreadMapEntry._2.fetcherStats.requestRate.oneMinuteRate.min(curMinAll);
        });
        }
        }
        },
        Map("clientId" -> clientId);
        );
    }

    // map of (source broker_id, fetcher_id per source broker) => fetcher;
private Map<BrokerAndFetcherId, AbstractFetcherThread> fetcherThreadMap = Maps.newHashMap();
private Object mapLock = new Object();




privatepublic Integer  void getFetcherId(String topic, Int partitionId)  {
        Utils.abs(31 * topic.hashCode() + partitionId) % numFetchers;
        }

        // to be defined in subclass to create a specific fetcher;
       public void createFetcherThread(Int fetcherId, Broker sourceBroker): AbstractFetcherThread;

       public void addFetcherForPartitions(Map partitionAndOffsets<TopicAndPartition, BrokerAndInitialOffset>) {
        mapLock synchronized {
        val partitionsPerFetcher = partitionAndOffsets.groupBy{ case(topicAndPartition, brokerAndInitialOffset) =>
        BrokerAndFetcherId(brokerAndInitialOffset.broker, getFetcherId(topicAndPartition.topic, topicAndPartition.partition))}
        for ((brokerAndFetcherId, partitionAndOffsets) <- partitionsPerFetcher) {
        var AbstractFetcherThread fetcherThread = null;
        fetcherThreadMap.get(brokerAndFetcherId) match {
        case Some(f) => fetcherThread = f;
        case None =>
        fetcherThread = createFetcherThread(brokerAndFetcherId.fetcherId, brokerAndFetcherId.broker);
        fetcherThreadMap.put(brokerAndFetcherId, fetcherThread);
        fetcherThread.start;
        }

        fetcherThreadMap(brokerAndFetcherId).addPartitions(partitionAndOffsets.map { case (topicAndPartition, brokerAndInitOffset) =>
        topicAndPartition -> brokerAndInitOffset.initOffset;
        });
        }
        }

        info(String.format("Added fetcher for partitions %s",partitionAndOffsets.map{ case (topicAndPartition, brokerAndInitialOffset) =>
        "<" + topicAndPartition + ", initOffset " + brokerAndInitialOffset.initOffset + " to broker " + brokerAndInitialOffset.broker + "> "}));
        }

       public void removeFetcherForPartitions(Set partitions<TopicAndPartition>) {
        mapLock synchronized {
        for ((key, fetcher) <- fetcherThreadMap) {
        fetcher.removePartitions(partitions);
        }
        }
        info(String.format("Removed fetcher for partitions %s",partitions.mkString(",")))
        }

       public void shutdownIdleFetcherThreads() {
        mapLock synchronized {
        val keysToBeRemoved = new mutable.HashSet<BrokerAndFetcherId>
        for ((key, fetcher) <- fetcherThreadMap) {
        if (fetcher.partitionCount <= 0) {
        fetcher.shutdown();
        keysToBeRemoved += key;
        }
        }
        fetcherThreadMap --= keysToBeRemoved;
        }
        }

       public void closeAllFetchers() {
        mapLock synchronized {
        for ( (_, fetcher) <- fetcherThreadMap) {
        fetcher.shutdown();
        }
        fetcherThreadMap.clear();
        }
        }
        }

        case class BrokerAndFetcherId(Broker broker, Int fetcherId);

        case class BrokerAndInitialOffset(Broker broker, Long initOffset);
