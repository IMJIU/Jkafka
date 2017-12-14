package kafka.consumer;

/**
 * @author zhoulf
 * @create 2017-12-14 47 15
 **/

import kafka.api.TopicMetadata;
import kafka.client.ClientUtils;
import kafka.cluster.Broker;
import kafka.cluster.Cluster;
import kafka.func.Tuple;
import kafka.log.TopicAndPartition;
import kafka.server.AbstractFetcherManager;
import kafka.server.AbstractFetcherThread;
import kafka.server.BrokerAndInitialOffset;
import kafka.utils.Sc;
import kafka.utils.ShutdownableThread;
import kafka.utils.Time;
import org.I0Itec.zkclient.ZkClient;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static kafka.utils.Utils.*;
import static kafka.utils.ZkUtils.*;

/**
 * Usage:
 * Once ConsumerFetcherManager is created, startConnections() and stopAllConnections() can be called repeatedly
 * until shutdown() is called.
 */
public class ConsumerFetcherManager extends AbstractFetcherManager {
    private String consumerIdString;
    private ConsumerConfig config;
    private ZkClient zkClient;

    public ConsumerFetcherManager(String consumerIdString, ConsumerConfig config, ZkClient zkClient) {
        super(String.format("ConsumerFetcherManager-%d", Time.get().milliseconds()), config.clientId, config.numConsumerFetchers);
        this.consumerIdString = consumerIdString;
        this.config = config;
        this.zkClient = zkClient;
    }

    private Map<TopicAndPartition, PartitionTopicInfo> partitionMap = null;
    private Cluster cluster = null;
    private HashSet<TopicAndPartition> noLeaderPartitionSet = new HashSet<TopicAndPartition>();
    private ReentrantLock lock = new ReentrantLock();
    private Condition cond = lock.newCondition();
    private ShutdownableThread leaderFinderThread = null;
    private AtomicInteger correlationId = new AtomicInteger(0);

    private class LeaderFinderThread extends ShutdownableThread {

        String name;

        public LeaderFinderThread(String name) {
            super(name);
        }


        // thread responsible for adding the fetcher to the right broker when leader is available;
        @Override
        public void doWork() {
            HashMap<TopicAndPartition, Broker> leaderForPartitionsMap = new HashMap<TopicAndPartition, Broker>();
            lock.lock();
            try {
                while (noLeaderPartitionSet.isEmpty()) {
                    trace("No partition for leader election.");
                    cond.await();
                }

                trace(String.format("Partitions without leader %s", noLeaderPartitionSet));
                List<Broker> brokers = getAllBrokersInCluster(zkClient);
                Set<TopicMetadata> topicsMetadata = Sc.toSet(ClientUtils.fetchTopicMetadata(Sc.map(noLeaderPartitionSet, m -> m.topic),
                        brokers,
                        config.clientId,
                        config.socketTimeoutMs,
                        correlationId.getAndIncrement()).topicsMetadata);
                if (logger.logger.isDebugEnabled())
                    topicsMetadata.forEach(topicMetadata -> debug(topicMetadata.toString()));
                topicsMetadata.forEach(tmd -> {
                    String topic = tmd.topic;
                    tmd.partitionsMetadata.forEach(pmd -> {
                        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, pmd.partitionId);
                        if (pmd.leader != null && noLeaderPartitionSet.contains(topicAndPartition)) {
                            Broker leaderBroker = pmd.leader;
                            leaderForPartitionsMap.put(topicAndPartition, leaderBroker);
                            noLeaderPartitionSet.remove(topicAndPartition);
                        }
                    });
                });
            } catch (Throwable t) {
                if (!isRunning.get())
                    throw new RuntimeException(t); /* If this thread is stopped, propagate this exception to kill the thread. */
                else
                    warn(String.format("Failed to find leader for %s", noLeaderPartitionSet), t);
            } finally {
                lock.unlock();
            }

            try {
                List<Tuple<TopicAndPartition, BrokerAndInitialOffset>> list = Sc.map(leaderForPartitionsMap, (topicAndPartition, broker) ->
                        Tuple.of(topicAndPartition, new BrokerAndInitialOffset(broker, partitionMap.get(topicAndPartition).getFetchOffset()))
                );
                addFetcherForPartitions(Sc.toMap(list));
            } catch (Throwable t2) {
                if (!isRunning.get())
                    throw t2; /* If this thread is stopped, propagate this exception to kill the thread. */
                else {
                    warn(String.format("Failed to add leader for partitions %s; will retry", Sc.mkString(leaderForPartitionsMap.keySet(), ",")), t2);
                    lock.lock();
                    noLeaderPartitionSet.addAll(leaderForPartitionsMap.keySet());
                    lock.unlock();
                }
            }

            shutdownIdleFetcherThreads();
            try {
                Thread.sleep(config.refreshLeaderBackoffMs);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public AbstractFetcherThread createFetcherThread(Integer fetcherId, Broker sourceBroker) {
        return new ConsumerFetcherThread(
                String.format("ConsumerFetcherThread-%s-%d-%d", consumerIdString, fetcherId, sourceBroker.id),
                config, sourceBroker, partitionMap, this);
    }

    public void startConnections(Iterable<PartitionTopicInfo> topicInfos, Cluster cluster) {
        leaderFinderThread = new LeaderFinderThread(consumerIdString + "-leader-finder-thread");
        leaderFinderThread.start();

        inLock(lock, () -> {
            List<Tuple<TopicAndPartition,PartitionTopicInfo>> list = Sc.map(topicInfos,tpi -> Tuple.of(new TopicAndPartition(tpi.topic, tpi.partitionId), tpi));
            partitionMap = Sc.toMap(list);
            this.cluster = cluster;
            noLeaderPartitionSet.addAll(Sc.map(topicInfos,tpi -> new TopicAndPartition(tpi.topic, tpi.partitionId)));
            cond.signalAll();
        });
    }

    public void stopConnections() {
    /*
     * Stop the leader finder thread first before stopping fetchers. Otherwise, if there are more partitions without
     * leader, then the leader finder thread will process these partitions (before shutting down) and add fetchers for
     * these partitions.
     */
        info("Stopping leader finder thread");
        if (leaderFinderThread != null) {
            leaderFinderThread.shutdown();
            leaderFinderThread = null;
        }

        info("Stopping all fetchers");
        closeAllFetchers();

        // no need to hold the lock for the following since leaderFindThread and all fetchers have been stopped;
        partitionMap = null;
        noLeaderPartitionSet.clear();

        info("All connections stopped");
    }

    public void addPartitionsWithError(Iterable<TopicAndPartition> partitionList) {
        debug(String.format("adding partitions with error %s", partitionList));
        inLock(lock, () -> {
            if (partitionMap != null) {
                partitionList.forEach(p -> noLeaderPartitionSet.add(p));
                cond.signalAll();
            }
        });
    }
}
