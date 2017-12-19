package kafka.consumer;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import kafka.api.OffsetCommitRequest;
import kafka.api.OffsetCommitResponse;
import kafka.api.OffsetFetchResponse;
import kafka.client.ClientUtils;
import kafka.cluster.Broker;
import kafka.cluster.Cluster;
import kafka.common.*;
import kafka.func.Handler;
import kafka.func.Tuple;
import kafka.log.TopicAndPartition;
import kafka.metrics.KafkaMetricsGroup;
import kafka.metrics.KafkaTimer;
import kafka.network.BlockingChannel;
import kafka.producer.KafkaMetricsReporter;
import kafka.serializer.Decoder;
import kafka.serializer.DefaultDecoder;
import kafka.utils.*;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.zookeeper.Watcher;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static kafka.utils.ZkUtils.*;

/**
 * This class handles the consumers interaction with zookeeper
 * <p>
 * Directories:
 * 1. Consumer id registry:
 * /consumers/<group_id>/ids<consumer_id> -> topic1,...topicN
 * A consumer has a unique consumer id within a consumer group. A consumer registers its id as an ephemeral znode
 * and puts all topics that it subscribes to as the value of the znode. The znode is deleted when the client is gone.
 * A consumer subscribes to event changes of the consumer id registry within its group.
 * <p>
 * The consumer id is picked up from configuration, instead of the sequential id assigned by ZK. Generated sequential
 * ids are hard to recover during temporary connection loss to ZK, since it's difficult for the client to figure out
 * whether the creation of a sequential znode has succeeded or not. More details can be found at
 * (http://wiki.apache.org/hadoop/ZooKeeper/ErrorHandling)
 * <p>
 * 2. Broker node registry:
 * /brokers/<0...N> --> { "host" : "port host",
 * "topics" : {"topic1": <"partition1" ... "partitionN">, ...,
 * "topicN": <"partition1" ... "partitionN"> } }
 * This is a list of all present broker brokers. A unique logical node id is configured on each broker node. A broker
 * node registers itself on start-up and creates a znode with the logical node id under /brokers. The value of the znode
 * is a JSON String that contains (1) the host name and the port the broker is listening to, (2) a list of topics that
 * the broker serves, (3) a list of logical partitions assigned to each topic on the broker.
 * A consumer subscribes to event changes of the broker node registry.
 * <p>
 * 3. Partition owner registry:
 * /consumers/<group_id>/owner/<topic>/<broker_id-partition_id> --> consumer_node_id
 * This stores the mapping before broker partitions and consumers. Each partition is owned by a unique consumer
 * within a consumer group. The mapping is reestablished after each rebalancing.
 * <p>
 * 4. Consumer offset tracking:
 * /consumers/<group_id>/offsets/<topic>/<broker_id-partition_id> --> offset_counter_value
 * Each consumer tracks the offset of the latest message consumed for each partition.
 */
public class ZookeeperConsumerConnector extends KafkaMetricsGroup implements ConsumerConnector {
    public static final FetchedDataChunk shutdownCommand = new FetchedDataChunk(null, null, -1L);
    public ConsumerConfig config;
    public Boolean enableFetcher;

    public ZookeeperConsumerConnector(ConsumerConfig config, Boolean enableFetcher) {
        this.config = config;
        this.enableFetcher = enableFetcher;
    }
    // for testing only;

    private AtomicBoolean isShuttingDown = new AtomicBoolean(false);
    private Object rebalanceLock = new Object();
    private Optional<ConsumerFetcherManager> fetcher = Optional.empty();
    private ZkClient zkClient = null;
    private Pool<String, Pool<Integer, PartitionTopicInfo>> topicRegistry = new Pool<>();
    private Pool<TopicAndPartition, Long> checkpointedZkOffsets = new Pool<TopicAndPartition, Long>();
    private Pool<Tuple<String, ConsumerThreadId>, BlockingQueue<FetchedDataChunk>> topicThreadIdAndQueues = new Pool<>();
    private KafkaScheduler scheduler = new KafkaScheduler(1, "kafka-consumer-scheduler-", true);
    private AtomicBoolean messageStreamCreated = new AtomicBoolean(false);

    private ZKSessionExpireListener sessionExpirationListener = null;
    private ZKTopicPartitionChangeListener topicPartitionChangeListener = null;
    private ZKRebalancerListener loadBalancerListener = null;

    private BlockingChannel offsetsChannel = null;
    private Object offsetsChannelLock = new Object();

    private ZookeeperTopicEventWatcher wildcardTopicWatcher = null;

    // useful for tracking migration of consumers to store offsets in kafka;
    private Meter kafkaCommitMeter = newMeter("KafkaCommitsPerSec", "commits", TimeUnit.SECONDS, ImmutableMap.of("clientId", config.clientId));
    private Meter zkCommitMeter = newMeter("ZooKeeperCommitsPerSec", "commits", TimeUnit.SECONDS, ImmutableMap.of("clientId", config.clientId));
    private KafkaTimer rebalanceTimer = new KafkaTimer(newTimer("RebalanceRateAndTime", TimeUnit.MILLISECONDS, TimeUnit.SECONDS, ImmutableMap.of("clientId", config.clientId)));
    public String consumerIdString;

    public void init() {

        String consumerUuid = null;
        if (config.consumerId.isPresent()) {
            consumerUuid = config.consumerId.get();
        } else {
            UUID uuid = UUID.randomUUID();
            try {
                consumerUuid = String.format("%s-%d-%s", InetAddress.getLocalHost().getHostName(), System.currentTimeMillis(),
                        Long.toHexString(uuid.getMostSignificantBits()).substring(0, 8));
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        }
        consumerIdString = config.groupId + "_" + consumerUuid;

        this.logIdent = "<" + consumerIdString + ">, ";

        connectZk();
        createFetcher();
        ensureOffsetManagerConnected();

        if (config.autoCommitEnable) {
            scheduler.startup();
            info("starting auto committer every " + config.autoCommitIntervalMs + " ms");
            scheduler.schedule("kafka-consumer-autocommit",
                    () -> autoCommit(),
                    config.autoCommitIntervalMs.longValue(),
                    config.autoCommitIntervalMs.longValue(),
                    TimeUnit.MILLISECONDS);
        }

        KafkaMetricsReporter.startReporters(config.props);
        AppInfo.registerInfo();
    }

    public ZookeeperConsumerConnector(ConsumerConfig config) {
        this(config, true);
    }

    public Map<String, List<KafkaStream<byte[], byte[]>>> createMessageStreams(Map<String, Integer> topicCountMap) {
        Map<String, List<KafkaStream<byte[], byte[]>>> streams = createMessageStreams(topicCountMap, new DefaultDecoder(), new DefaultDecoder());
        return streams;
    }

    public <K, V> Map<String, List<KafkaStream<K, V>>> createMessageStreams(Map<String, Integer> topicCountMap, Decoder<K> keyDecoder, Decoder<V> valueDecoder) {
        if (messageStreamCreated.getAndSet(true))
            throw new MessageStreamsExistException(this.getClass().getSimpleName() + " can create message streams at most once", null);
        return consume(topicCountMap, keyDecoder, valueDecoder);
    }

    public <K, V> List<KafkaStream<K, V>> createMessageStreamsByFilter(TopicFilter topicFilter,
                                                                       Integer numStreams,
                                                                       Decoder<K> keyDecoder,
                                                                       Decoder<V> valueDecoder) {
        if (keyDecoder == null) keyDecoder = (Decoder<K>) new DefaultDecoder();
        if (valueDecoder == null) valueDecoder = (Decoder<V>) new DefaultDecoder();
        WildcardStreamsHandler<K, V> wildcardStreamsHandler = new WildcardStreamsHandler<>(topicFilter, numStreams, keyDecoder, valueDecoder);
        return wildcardStreamsHandler.streams();
    }

    private void createFetcher() {
        if (enableFetcher)
            fetcher = Optional.of(new ConsumerFetcherManager(consumerIdString, config, zkClient));
    }

    private void connectZk() {
        info("Connecting to zookeeper instance at " + config.zkConnect);
        zkClient = new ZkClient(config.zkConnect, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs, new ZKStringSerializer());
    }

    // Blocks until the offset manager is located and a channel is established to it.;
    private void ensureOffsetManagerConnected() {
        if (config.offsetsStorage == "kafka") {
            if (offsetsChannel == null || !offsetsChannel.isConnected())
                offsetsChannel = ClientUtils.channelToOffsetManager(config.groupId, zkClient, config.offsetsChannelSocketTimeoutMs, config.offsetsChannelBackoffMs);

            debug(String.format("Connected to offset manager %s:%d.", offsetsChannel.host, offsetsChannel.port));
        }
    }

    public void shutdown() {
        boolean canShutdown = isShuttingDown.compareAndSet(false, true);
        if (canShutdown) {
            info("ZKConsumerConnector shutting down");
            long startTime = System.nanoTime();
            KafkaMetricsGroup.removeAllConsumerMetrics(config.clientId);
            synchronized (rebalanceLock) {
                if (wildcardTopicWatcher != null)
                    wildcardTopicWatcher.shutdown();
                try {
                    if (config.autoCommitEnable)
                        scheduler.shutdown();
                    if (fetcher.isPresent()) {
                        fetcher.get().stopConnections();
                    }
                    sendShutdownToAllQueues();
                    if (config.autoCommitEnable)
                        commitOffsets(true);
                    if (zkClient != null) {
                        zkClient.close();
                        zkClient = null;
                    }

                    if (offsetsChannel != null) offsetsChannel.disconnect();
                } catch (Throwable e) {
                    error("error during consumer connector shutdown", e);
                }
                info("ZKConsumerConnector shutdown completed in " + (System.nanoTime() - startTime) / 1000000 + " ms");
            }
        }
    }

    public <K, V> Map<String, List<KafkaStream<K, V>>> consume(Map<String, Integer> topicCountMap, Decoder<K> keyDecoder, Decoder<V> valueDecoder) {
        debug("entering consume ");
        if (topicCountMap == null)
            throw new RuntimeException("topicCountMap is null");

        TopicCount topicCount = TopicCount.constructTopicCount(consumerIdString, topicCountMap);

        Map<String, Set<ConsumerThreadId>> topicThreadIds = topicCount.getConsumerThreadIdsPerTopic();

        // make a list of (queue,stream) pairs, one pair for each threadId;
        List<Set<Tuple<LinkedBlockingQueue<FetchedDataChunk>, KafkaStream<K, V>>>> list = Sc.map(topicThreadIds.values(), threadIdSet ->
                Sc.map(threadIdSet, t -> {
                    LinkedBlockingQueue<FetchedDataChunk> queue = new LinkedBlockingQueue<FetchedDataChunk>(config.queuedMaxMessages);
                    KafkaStream<K, V> stream = new KafkaStream<>(
                            queue, config.consumerTimeoutMs, keyDecoder, valueDecoder, config.clientId);
                    return Tuple.of(queue, stream);
                })
        );
        List<Tuple<LinkedBlockingQueue<FetchedDataChunk>, KafkaStream<K, V>>> queuesAndStreams = Lists.newArrayList();
        list.forEach(d -> queuesAndStreams.addAll(d));

        ZKGroupDirs dirs = new ZKGroupDirs(config.groupId);
        registerConsumerInZK(dirs, consumerIdString, topicCount);
        reinitializeConsumer(topicCount, queuesAndStreams);

        return loadBalancerListener.kafkaMessageAndMetadataStreams;
    }

    // this API is used by unit tests only;
    public Pool<String, Pool<Integer, PartitionTopicInfo>> getTopicRegistry = topicRegistry;

    private void registerConsumerInZK(ZKGroupDirs dirs, String consumerIdString, TopicCount topicCount) {
        info("begin registering consumer " + consumerIdString + " in ZK");
        String timestamp = Time.get().milliseconds().toString();
        String consumerRegistrationInfo = JSON.toJSONString(ImmutableMap.of("version", 1, "subscription",
                topicCount.getTopicCountMap(), "pattern", topicCount.pattern(),
                "timestamp", timestamp));

        ZkUtils.createEphemeralPathExpectConflictHandleZKBug(zkClient, dirs.consumerRegistryDir() + "/" + consumerIdString, consumerRegistrationInfo, null,
                (consumerZKString, consumer) -> true, config.zkSessionTimeoutMs);
        info("end registering consumer " + consumerIdString + " in ZK");
    }

    private void sendShutdownToAllQueues() {
        try {
            for (BlockingQueue<FetchedDataChunk> queue : Sc.toSet(topicThreadIdAndQueues.values())) {
                debug("Clearing up queue");
                queue.clear();
                queue.put(ZookeeperConsumerConnector.shutdownCommand);
                debug("Cleared queue and sent shutdown command");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void autoCommit() {
        trace("auto committing");
        try {
            commitOffsets(false);
        } catch (Throwable t) {
            // log it and let it go;
            error("exception during autoCommit: ", t);
        }
    }

    public void commitOffsetToZooKeeper(TopicAndPartition topicPartition, Long offset) {
        if (checkpointedZkOffsets.get(topicPartition) != offset) {
            ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs(config.groupId, topicPartition.topic);
            updatePersistentPath(zkClient, topicDirs.consumerOffsetDir() + "/" + topicPartition.partition, offset.toString());
            checkpointedZkOffsets.put(topicPartition, offset);
            zkCommitMeter.mark();
        }
    }

    public void commitOffsets(Boolean isAutoCommit) {
        int retriesRemaining = 1 + ((isAutoCommit) ? config.offsetsCommitMaxRetries : 0); // no retries for commits from auto-commit;
        boolean done = false;

        while (!done) {
            boolean committed;
            synchronized (offsetsChannelLock) { // committed when we receive either no error codes or only MetadataTooLarge errors;
                List<Tuple<TopicAndPartition, OffsetAndMetadata>> offsetsToCommitList = Sc.flatMap(topicRegistry, t -> {
                    List<Tuple<TopicAndPartition, OffsetAndMetadata>> list = Sc.map(t.v2, partitionInfos ->
                            Tuple.of(new TopicAndPartition(partitionInfos.v2.topic, partitionInfos.v2.partitionId),
                                    new OffsetAndMetadata(partitionInfos.v2.getConsumeOffset()))
                    );
                    return list;
                });
                Map<TopicAndPartition, OffsetAndMetadata> offsetsToCommit = Sc.toMap(offsetsToCommitList);

                if (offsetsToCommit.size() > 0) {
                    if (config.offsetsStorage == "zookeeper") {
                        offsetsToCommit.forEach((topicAndPartition, offsetAndMetadata) ->
                                commitOffsetToZooKeeper(topicAndPartition, offsetAndMetadata.offset));
                        committed = true;
                    } else {
                        OffsetCommitRequest offsetCommitRequest = new OffsetCommitRequest(config.groupId, offsetsToCommit);
                        ensureOffsetManagerConnected();
                        try {
                            kafkaCommitMeter.mark(offsetsToCommit.size());
                            offsetsChannel.send(offsetCommitRequest);
                            OffsetCommitResponse offsetCommitResponse = OffsetCommitResponse.readFrom(offsetsChannel.receive().buffer());
                            trace(String.format("Offset commit response: %s.", offsetCommitResponse));
                            boolean commitFailed = false, retryableIfFailed = false, shouldRefreshCoordinator = false;
                            short errorCount = 0;
                            for (Map.Entry<TopicAndPartition, Short> entry : offsetCommitResponse.commitStatus.entrySet()) {
                                TopicAndPartition topicAndPartition = entry.getKey();
                                Short errorCode = entry.getValue();
                                commitFailed = commitFailed || errorCode != ErrorMapping.NoError;// update commitFailed;
                                // update retryableIfFailed - (only metadata too large is not retryable);
                                retryableIfFailed = retryableIfFailed || (errorCode != ErrorMapping.NoError && errorCode != ErrorMapping.OffsetMetadataTooLargeCode);
                                shouldRefreshCoordinator = shouldRefreshCoordinator || errorCode == ErrorMapping.NotCoordinatorForConsumerCode
                                        || errorCode == ErrorMapping.ConsumerCoordinatorNotAvailableCode;
                                errorCount += (errorCount != ErrorMapping.NoError) ? new Integer(1).shortValue() : new Integer(0).shortValue();  // update error count;
                            }
                            debug(errorCount + " errors in offset commit response.");


                            if (shouldRefreshCoordinator) {
                                debug("Could not commit offsets (because offset coordinator has moved or is unavailable).");
                                offsetsChannel.disconnect();
                            }

                            if (commitFailed && retryableIfFailed)
                                committed = false;
                            else
                                committed = true;
                        } catch (Throwable t) {
                            error("Error while committing offsets.", t);
                            offsetsChannel.disconnect();
                            committed = false;
                        }
                    }
                } else {
                    debug("No updates to offsets since last commit.");
                    committed = true;
                }
            }

            if (isShuttingDown.get() && isAutoCommit) { // should not retry indefinitely if shutting down;
                retriesRemaining -= 1;
                done = retriesRemaining == 0 || committed;
            } else
                done = true;

            if (!done) {
                debug(String.format("Retrying offset commit in %d ms", config.offsetsChannelBackoffMs));
                Utils.swallow(() -> Thread.sleep(config.offsetsChannelBackoffMs));
            }
        }
    }

    /**
     * KAFKA-This 1743 method added for backward compatibility.
     */
    public void commitOffsets() {
        commitOffsets(true);
    }

    private Handler<TopicAndPartition, Tuple<TopicAndPartition, OffsetMetadataAndError>> fetchOffsetFromZooKeeper = (topicPartition) -> {
        ZKGroupTopicDirs dirs = new ZKGroupTopicDirs(config.groupId, topicPartition.topic);
        Optional<String> offsetString = readDataMaybeNull(zkClient, dirs.consumerOffsetDir() + "/" + topicPartition.partition).v1;
        if (offsetString.isPresent()) {
            return Tuple.of(topicPartition, new OffsetMetadataAndError(Long.parseLong(offsetString.get()), OffsetAndMetadata.NoMetadata, ErrorMapping.NoError));
        } else {
            return Tuple.of(topicPartition, OffsetMetadataAndError.NoOffset);
        }
    };

    private Optional<OffsetFetchResponse> fetchOffsets(List<TopicAndPartition> partitions) {
        if (partitions.isEmpty())
            return Optional.of(new OffsetFetchResponse(Collections.EMPTY_MAP));
        else if (config.offsetsStorage == "zookeeper") {
            List<Tuple<TopicAndPartition, OffsetMetadataAndError>> offsets = Sc.map(partitions, p -> fetchOffsetFromZooKeeper.handle(p));
            return Optional.of(new kafka.api.OffsetFetchResponse(Sc.toMap(offsets)));
        } else {
            kafka.api.OffsetFetchRequest offsetFetchRequest = new kafka.api.OffsetFetchRequest(config.groupId, partitions, config.clientId);

            Optional<OffsetFetchResponse> offsetFetchResponseOpt = Optional.empty();
            while (!isShuttingDown.get() && !offsetFetchResponseOpt.isPresent()) {
                synchronized (offsetsChannelLock) {
                    ensureOffsetManagerConnected();
                    try {
                        offsetsChannel.send(offsetFetchRequest);
                        OffsetFetchResponse offsetFetchResponse = OffsetFetchResponse.readFrom(offsetsChannel.receive().buffer());
                        trace(String.format("Offset fetch response: %s.", offsetFetchResponse));
                        boolean leaderChanged = false, loadInProgress = false;
                        for (Map.Entry<TopicAndPartition, OffsetMetadataAndError> entry : offsetFetchResponse.requestInfo.entrySet()) {
                            leaderChanged = (leaderChanged || (entry.getValue().error == ErrorMapping.NotCoordinatorForConsumerCode));
                            loadInProgress = loadInProgress || (entry.getValue().error == ErrorMapping.OffsetsLoadInProgressCode);
                        }

                        if (leaderChanged) {
                            offsetsChannel.disconnect();
                            debug("Could not fetch offsets (because offset manager has moved).");
                            offsetFetchResponseOpt = Optional.empty(); // retry;
                        } else if (loadInProgress) {
                            debug("Could not fetch offsets (because offset cache is being loaded).");
                            offsetFetchResponseOpt = Optional.empty(); // retry;
                        } else {
                            if (config.dualCommitEnabled) {
                                // if dual-commit is enabled (i.e., if a consumer group is migrating offsets to kafka), then pick the;
                                // maximum between offsets in zookeeper and kafka.;
                                Map<TopicAndPartition, OffsetMetadataAndError> kafkaOffsets = offsetFetchResponse.requestInfo;
                                List<Tuple<TopicAndPartition, OffsetMetadataAndError>> mostRecentOffsets = Sc.map(kafkaOffsets, (topicPartition, kafkaOffset) -> {
                                    Long zkOffset = fetchOffsetFromZooKeeper.handle(topicPartition).v2.offset;
                                    Long mostRecentOffset = Math.max(zkOffset, kafkaOffset.offset);
                                    return Tuple.of(topicPartition, new OffsetMetadataAndError(mostRecentOffset, kafkaOffset.metadata, ErrorMapping.NoError));
                                });
                                offsetFetchResponseOpt = Optional.of(new OffsetFetchResponse(Sc.toMap(mostRecentOffsets)));
                            } else
                                offsetFetchResponseOpt = Optional.of(offsetFetchResponse);
                        }
                    } catch (Exception e) {
                        warn(String.format("Error while fetching offsets from %s:%d. Possible cause: %s", offsetsChannel.host, offsetsChannel.port, e.getMessage()));
                        offsetsChannel.disconnect();
                        offsetFetchResponseOpt = Optional.empty(); // retry;
                    }
                }

                if (!offsetFetchResponseOpt.isPresent()) {
                    debug(String.format("Retrying offset fetch in %d ms", config.offsetsChannelBackoffMs));
                    Utils.swallow(() -> Thread.sleep(config.offsetsChannelBackoffMs));
                }
            }

            return offsetFetchResponseOpt;
        }
    }

    class ZKSessionExpireListener implements IZkStateListener {
        public ZKGroupDirs dirs;
        public String consumerIdString;
        public TopicCount topicCount;
        public ZKRebalancerListener loadBalancerListener;

        public ZKSessionExpireListener(ZKGroupDirs dirs, java.lang.String consumerIdString, TopicCount topicCount, ZookeeperConsumerConnector.ZKRebalancerListener loadBalancerListener) {
            this.dirs = dirs;
            this.consumerIdString = consumerIdString;
            this.topicCount = topicCount;
            this.loadBalancerListener = loadBalancerListener;
        }

        //@throws(classOf<Exception>)
        public void handleStateChanged(Watcher.Event.KeeperState state) {
            // do nothing, since zkclient will do reconnect for us.;
        }

        /**
         * Called after the zookeeper session has expired and a new session has been created. You would have to re-create
         * any ephemeral nodes here.
         *
         * @throws Exception On any error.
         */
//        @throws(classOf<Exception>)
        public void handleNewSession() {
            /**
             *  When we get a SessionExpired event, we lost all ephemeral nodes and zkclient has reestablished a
             *  connection for us. We need to release the ownership of the current consumer and re-register this
             *  consumer in the consumer registry and trigger a rebalance.
             */
            info("ZK expired; release old broker parition ownership; re-register consumer " + consumerIdString);
            loadBalancerListener.resetState();
            registerConsumerInZK(dirs, consumerIdString, topicCount);
            // explicitly trigger load balancing for this consumer;
            loadBalancerListener.syncedRebalance();
            // There is no need to resubscribe to child and state changes.;
            // The child change watchers will be set inside rebalance when we read the children list.;
        }

    }

    class ZKTopicPartitionChangeListener implements IZkDataListener {
        ZKRebalancerListener loadBalancerListener;

        public ZKTopicPartitionChangeListener(ZKRebalancerListener loadBalancerListener) {
            this.loadBalancerListener = loadBalancerListener;
        }

        public void handleDataChange(String dataPath, Object data) {
            try {
                info("Topic info for path " + dataPath + " changed to " + data.toString() + ", triggering rebalance");
                // queue up the rebalance event;
                loadBalancerListener.rebalanceEventTriggered();
                // There is no need to re-subscribe the watcher since it will be automatically;
                // re-registered upon firing of this event by zkClient;
            } catch (Throwable e) {
                error("Error while handling topic partition change for data path " + dataPath, e);
            }
        }

        //        @throws(classOf<Exception>)
        public void handleDataDeleted(String dataPath) {
            // This TODO need to be implemented when we support delete topic;
            warn("Topic for path " + dataPath + " gets deleted, which should not happen at this time");
        }
    }

    class ZKRebalancerListener<K, V> implements IZkChildListener {
        public String group;
        public String consumerIdString;
        public Map<String, List<KafkaStream<K, V>>> kafkaMessageAndMetadataStreams;

        public ZKRebalancerListener(String group, String consumerIdString, Map<String, List<KafkaStream<K, V>>> kafkaMessageAndMetadataStreams) {
            this.group = group;
            this.consumerIdString = consumerIdString;
            this.kafkaMessageAndMetadataStreams = kafkaMessageAndMetadataStreams;
            newGauge("OwnedPartitionsCount", new Gauge<Integer>() {
                public Integer value() {
                    return allTopicsOwnedPartitionsCount;
                }
            }, ImmutableMap.of("clientId", config.clientId, "groupId", config.groupId));
            watcherExecutorThread.start();
        }

        private PartitionAssignor partitionAssignor = PartitionAssignor.createInstance(config.partitionAssignmentStrategy);

        private boolean isWatcherTriggered = false;
        private ReentrantLock lock = new ReentrantLock();
        private Condition cond = lock.newCondition();

        private volatile int allTopicsOwnedPartitionsCount = 0;

        private Map<String, String> ownedPartitionsCountMetricTags(String topic) {
            return ImmutableMap.of("clientId", config.clientId, "groupId", config.groupId, "topic", topic);

        }

        private Thread watcherExecutorThread = new Thread(consumerIdString + "_watcher_executor") {
            @Override
            public void run() {
                info("starting watcher executor thread for consumer " + consumerIdString);
                boolean doRebalance = false;
                while (!isShuttingDown.get()) {
                    try {
                        lock.lock();
                        try {
                            if (!isWatcherTriggered)
                                cond.await(1000, TimeUnit.MILLISECONDS); // wake up periodically so that it can check the shutdown flag;
                        } finally {
                            doRebalance = isWatcherTriggered;
                            isWatcherTriggered = false;
                            lock.unlock();
                        }
                        if (doRebalance)
                            syncedRebalance();
                    } catch (Throwable t) {
                        error("error during syncedRebalance", t);
                    }
                }
                info("stopping watcher executor thread for consumer " + consumerIdString);
            }
        };


        //        @throws(classOf<Exception>)
        public void handleChildChange(String parentPath, java.util.List<String> curChilds) {
            rebalanceEventTriggered();
        }

        public void rebalanceEventTriggered() {
            Utils.inLock(lock, () -> {
                isWatcherTriggered = true;
                cond.signalAll();
            });
        }

        private void deletePartitionOwnershipFromZK(String topic, Integer partition) {
            ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs(group, topic);
            String znode = topicDirs.consumerOwnerDir() + "/" + partition;
            deletePath(zkClient, znode);
            debug("Consumer " + consumerIdString + " releasing " + znode);
        }

        private void releasePartitionOwnership(Pool<String, Pool<Integer, PartitionTopicInfo>> localTopicRegistry) {
            info("Releasing partition ownership");
            for (Tuple<String, Pool<Integer, PartitionTopicInfo>> tuple : localTopicRegistry) {
                String topic = tuple.v1;
                Pool<Integer, PartitionTopicInfo> infos = tuple.v2;
                for (Integer partition : infos.keys()) {
                    deletePartitionOwnershipFromZK(topic, partition);
                }
                removeMetric("OwnedPartitionsCount", ownedPartitionsCountMetricTags(topic));
                localTopicRegistry.remove(topic);
            }
            allTopicsOwnedPartitionsCount = 0;
        }

        public void resetState() {
            topicRegistry.clear();
        }

        public void syncedRebalance() {
            synchronized (rebalanceLock) {
                rebalanceTimer.time(() -> {
                    if (isShuttingDown.get()) {
                        return null;
                    } else {
                        for (int i = 0; i < config.rebalanceMaxRetries; i++) {
                            info("begin rebalancing consumer " + consumerIdString + " try #" + i);
                            boolean done = false;
                            Cluster cluster = null;
                            try {
                                cluster = getCluster(zkClient);
                                done = rebalance(cluster);
                            } catch (Throwable e) {
                                /** occasionally, we may hit a ZK exception because the ZK state is changing while we are iterating.
                                 * For example, a ZK node can disappear between the time we get all children and the time we try to get
                                 * the value of a child. Just let this go since another rebalance will be triggered.
                                 **/
                                info("exception during rebalance ", e);
                            }
                            info("end rebalancing consumer " + consumerIdString + " try #" + i);
                            if (done) {
                                return null;
                            } else {
                /* Here the cache is at a risk of being stale. To take future rebalancing decisions correctly, we should
                 * clear the cache */
                                info("Rebalancing attempt failed. Clearing the cache before the next rebalancing operation is triggered");
                            }
                            // stop all fetchers and clear all the queues to avoid data duplication;
                            closeFetchersForQueues(cluster, kafkaMessageAndMetadataStreams, Sc.map(topicThreadIdAndQueues, q -> q.v2));
                            Utils.swallowError(() -> Thread.sleep(config.rebalanceBackoffMs));
                        }
                        return null;
                    }
                });
            }

            throw new ConsumerRebalanceFailedException(consumerIdString + " can't rebalance after " + config.rebalanceMaxRetries + " retries");
        }

        private Boolean rebalance(Cluster cluster) {
            Map<String, Set<ConsumerThreadId>> myTopicThreadIdsMap = TopicCount.constructTopicCount(
                    group, consumerIdString, zkClient, config.excludeInternalTopics).getConsumerThreadIdsPerTopic();
            List<Broker> brokers = getAllBrokersInCluster(zkClient);
            if (brokers.size() == 0) {
                // This can happen in a rare case when there are no brokers available in the cluster when the consumer is started.;
                // We log an warning and register for child changes on brokers/id so that rebalance can be triggered when the brokers;
                // are up.;
                warn("no brokers found when trying to rebalance.");
                zkClient.subscribeChildChanges(ZkUtils.BrokerIdsPath, loadBalancerListener);
                return true;
            } else {
                /**
                 * fetchers must be stopped to avoid data duplication, since if the current
                 * rebalancing attempt fails, the partitions that are released could be owned by another consumer.
                 * But if we don't stop the fetchers first, this consumer would continue returning data for released
                 * partitions in parallel. So, not stopping the fetchers leads to duplicate data.
                 */
                closeFetchers(cluster, kafkaMessageAndMetadataStreams, myTopicThreadIdsMap);

                releasePartitionOwnership(topicRegistry);

                AssignmentContext assignmentContext = new AssignmentContext(group, consumerIdString, config.excludeInternalTopics, zkClient);
                Map<TopicAndPartition, ConsumerThreadId> partitionOwnershipDecision = partitionAssignor.assign(assignmentContext);
                Pool<String, Pool<Integer, PartitionTopicInfo>> currentTopicRegistry = new Pool<>(
                        Optional.of((String topic) -> new Pool<Integer, PartitionTopicInfo>()));

                // fetch current offsets for all topic-partitions;
                List<TopicAndPartition> topicPartitions = Sc.toList(partitionOwnershipDecision.keySet());

                Optional<OffsetFetchResponse> offsetFetchResponseOpt = fetchOffsets(topicPartitions);

                if (isShuttingDown.get() || !offsetFetchResponseOpt.isPresent())
                    return false;
                else {
                    kafka.api.OffsetFetchResponse offsetFetchResponse = offsetFetchResponseOpt.get();
                    topicPartitions.forEach(topicAndPartition -> {
                        Long offset = offsetFetchResponse.requestInfo.get(topicAndPartition).offset;
                        ConsumerThreadId threadId = partitionOwnershipDecision.get(topicAndPartition);
                        addPartitionTopicInfo(currentTopicRegistry, topicAndPartition.partition, topicAndPartition.topic, offset, threadId);
                    });

                    /**
                     * move the partition ownership here, since that can be used to indicate a truly successful rebalancing attempt
                     * A rebalancing attempt is completed successfully only after the fetchers have been started correctly
                     */
                    if (reflectPartitionOwnershipDecision(partitionOwnershipDecision)) {
                        allTopicsOwnedPartitionsCount = partitionOwnershipDecision.size();
                        Map<String, Map<TopicAndPartition, ConsumerThreadId>> map = Sc.groupByKey(partitionOwnershipDecision, k -> k.topic);
                        map.forEach((topic, partitionThreadPairs) ->
                                newGauge("OwnedPartitionsCount",
                                        new Gauge<Integer>() {
                                            public Integer value() {
                                                return partitionThreadPairs.size();
                                            }
                                        },
                                        ownedPartitionsCountMetricTags(topic))
                        );
                        topicRegistry = currentTopicRegistry;
                        updateFetcher(cluster);
                        return true;
                    } else {
                        return false;
                    }
                }
            }
        }

        private void closeFetchersForQueues(Cluster cluster,
                                            Map<String, List<KafkaStream<K, V>>> messageStreams,
                                            Iterable<BlockingQueue<FetchedDataChunk>> queuesToBeCleared) {
            List<PartitionTopicInfo> allPartitionInfos = Sc.flatten(Sc.map(topicRegistry.values(), p -> p.values()));
            if (fetcher.isPresent()) {
                fetcher.get().stopConnections();
                clearFetcherQueues(allPartitionInfos, cluster, queuesToBeCleared, messageStreams);
                info("Committing all offsets after clearing the fetcher queues");
                /**
                 * here, we need to commit offsets before stopping the consumer from returning any more messages
                 * from the current data chunk. Since partition ownership is not yet released, this commit offsets
                 * call will ensure that the offsets committed now will be used by the next consumer thread owning the partition
                 * for the current data chunk. Since the fetchers are already shutdown and this is the last chunk to be iterated
                 * by the consumer, there will be no more messages returned by this iterator until the rebalancing finishes
                 * successfully and the fetchers restart to fetch more data chunks
                 **/
                if (config.autoCommitEnable)
                    commitOffsets(true);
            }
        }

        private void clearFetcherQueues(Iterable<PartitionTopicInfo> topicInfos, Cluster cluster,
                                        Iterable<BlockingQueue<FetchedDataChunk>> queuesTobeCleared,
                                        Map<String, List<KafkaStream<K, V>>> messageStreams) {

            // Clear all but the currently iterated upon chunk in the consumer thread's queue;
            queuesTobeCleared.forEach(q -> q.clear());
            info("Cleared all relevant queues for this fetcher");

            // Also clear the currently iterated upon chunk in the consumer threads;
            if (messageStreams != null)
                messageStreams.forEach((k, v) -> v.forEach(s -> s.clear()));

            info("Cleared the data chunks in all the consumer message iterators");

        }

        private void closeFetchers(Cluster cluster, Map<String, List<KafkaStream<K, V>>> messageStreams,
                                   Map<String, Set<ConsumerThreadId>> relevantTopicThreadIdsMap) {
            // only clear the fetcher queues for certain topic partitions that *might* no longer be served by this consumer;
            // after this rebalancing attempt;
            List<BlockingQueue<FetchedDataChunk>> queuesTobeCleared = Sc.map(Sc.filter(topicThreadIdAndQueues, q -> relevantTopicThreadIdsMap.containsKey(q.v1.v1)), q -> q.v2);
            closeFetchersForQueues(cluster, messageStreams, queuesTobeCleared);
        }

        private void updateFetcher(Cluster cluster) {
            // update partitions for fetcher;
            List<PartitionTopicInfo> allPartitionInfos = null;
            for (Pool<Integer, PartitionTopicInfo> partitionInfos : topicRegistry.values())
                for (PartitionTopicInfo partition : partitionInfos.values())
                    allPartitionInfos.add(partition);
            info("Consumer " + consumerIdString + " selected partitions : " +
                    Sc.mkString(Sc.map(Sc.sortWith(allPartitionInfos, (s, t) -> s.partitionId < t.partitionId), p -> p.toString()), ","));

            if (fetcher.isPresent())
                fetcher.get().startConnections(allPartitionInfos, cluster);
        }

        private Boolean reflectPartitionOwnershipDecision(Map<TopicAndPartition, ConsumerThreadId> partitionOwnershipDecision) {
            List<Tuple<String, Integer>> successfullyOwnedPartitions = null;
            List<Boolean> partitionOwnershipSuccessful = Sc.map(partitionOwnershipDecision, (k, v) -> {
                String topic = k.topic;
                Integer partition = k.partition;
                ConsumerThreadId consumerThreadId = v;
                String partitionOwnerPath = getConsumerPartitionOwnerPath(group, topic, partition);
                try {
                    createEphemeralPathExpectConflict(zkClient, partitionOwnerPath, consumerThreadId.toString());
                    info(consumerThreadId + " successfully owned partition " + partition + " for topic " + topic);
                    successfullyOwnedPartitions.add(Tuple.of(topic, partition));
                    return true;
                } catch (ZkNodeExistsException e) {
                    // The node hasn't been deleted by the original owner. So wait a bit and retry.;
                    info("waiting for the partition ownership to be deleted: " + partition);
                    return false;
                } catch (Throwable e2) {
                    throw e2;
                }
            });
            Integer hasPartitionOwnershipFailed = 0;
            for (Boolean b : partitionOwnershipSuccessful) {
                if (!b) {
                    hasPartitionOwnershipFailed += 1;
                }
            }
      /* even if one of the partition ownership attempt has failed, return false */
            if (hasPartitionOwnershipFailed > 0) {
                // remove all paths that we have owned in ZK;
                successfullyOwnedPartitions.forEach(topicAndPartition -> deletePartitionOwnershipFromZK(topicAndPartition.v1, topicAndPartition.v2));
                return false;
            } else
                return true;
        }

        private void addPartitionTopicInfo(Pool<String, Pool<Integer, PartitionTopicInfo>> currentTopicRegistry,
                                           Integer partition, String topic,
                                           Long offset, ConsumerThreadId consumerThreadId) {
            Pool<Integer, PartitionTopicInfo> partTopicInfoMap = currentTopicRegistry.getAndMaybePut(topic);

            BlockingQueue<FetchedDataChunk> queue = topicThreadIdAndQueues.get(Tuple.of(topic, consumerThreadId));
            AtomicLong consumedOffset = new AtomicLong(offset);
            AtomicLong fetchedOffset = new AtomicLong(offset);
            PartitionTopicInfo partTopicInfo = new PartitionTopicInfo(topic,
                    partition,
                    queue,
                    consumedOffset,
                    fetchedOffset,
                    new AtomicInteger(config.fetchMessageMaxBytes),
                    config.clientId);
            partTopicInfoMap.put(partition, partTopicInfo);
            debug(partTopicInfo + " selected new offset " + offset);
            checkpointedZkOffsets.put(new TopicAndPartition(topic, partition), offset);
        }
    }

    private <K, V> void reinitializeConsumer(
            TopicCount topicCount,
            List<Tuple<LinkedBlockingQueue<FetchedDataChunk>, KafkaStream<K, V>>> queuesAndStreams) {

        ZKGroupDirs dirs = new ZKGroupDirs(config.groupId);

        // listener to consumer and partition changes;
        if (loadBalancerListener == null) {
            HashMap<String, List<KafkaStream<K, V>>> topicStreamsMap = new HashMap<>();
            loadBalancerListener = new ZKRebalancerListener(
                    config.groupId, consumerIdString, topicStreamsMap);
        }

        // create listener for session expired event if not exist yet;
        if (sessionExpirationListener == null)
            sessionExpirationListener = new ZKSessionExpireListener(
                    dirs, consumerIdString, topicCount, loadBalancerListener);

        // create listener for topic partition change event if not exist yet;
        if (topicPartitionChangeListener == null)
            topicPartitionChangeListener = new ZKTopicPartitionChangeListener(loadBalancerListener);

        Map<String, List<KafkaStream<K, V>>> topicStreamsMap = loadBalancerListener.kafkaMessageAndMetadataStreams;

        // map of {topic -> Set(thread-1, thread-2, ...)}
        Map<String, Set<ConsumerThreadId>> consumerThreadIdsPerTopic = topicCount.getConsumerThreadIdsPerTopic();
        /*
         * Wild-card consumption streams share the same queues, so we need to
         * duplicate the list for the subsequent zip operation.
          */
        List<Tuple<LinkedBlockingQueue<FetchedDataChunk>, KafkaStream<K, V>>> allQueuesAndStreams = null;
        if (topicCount instanceof WildcardTopicCount) {
            allQueuesAndStreams = Sc.itFlatToList(1, consumerThreadIdsPerTopic.keySet().size(), n -> queuesAndStreams.stream());
        } else if (topicCount instanceof StaticTopicCount) {
            allQueuesAndStreams = queuesAndStreams;
        }
        List<Set<Tuple<String, ConsumerThreadId>>> list = Sc.map(consumerThreadIdsPerTopic, (topic, threadIds) -> Sc.map(threadIds, t -> Tuple.of(topic, t)));
        List<Tuple<String, ConsumerThreadId>> topicThreadIds = Lists.newArrayList();
        for (Set<Tuple<String, ConsumerThreadId>> collection : list) {
            for (Tuple<String, ConsumerThreadId> i : collection) {
                topicThreadIds.add(i);
            }
        }

        Prediction.require(topicThreadIds.size() == allQueuesAndStreams.size(),
                String.format("Mismatch between thread ID count (%d) and queue count (%d)",
                        topicThreadIds.size(), allQueuesAndStreams.size()));
        Sc.zipWithIndex(allQueuesAndStreams);
        List<Tuple<Tuple<String, ConsumerThreadId>, Tuple<LinkedBlockingQueue<FetchedDataChunk>, KafkaStream<K, V>>>> threadQueueStreamPairs = Sc.zip(topicThreadIds, allQueuesAndStreams);

        threadQueueStreamPairs.forEach(e -> {
            Tuple<String, ConsumerThreadId> topicThreadId = e.v1;
            LinkedBlockingQueue<FetchedDataChunk> q = e.v2.v1;
            topicThreadIdAndQueues.put(topicThreadId, q);
            debug(String.format("Adding topicThreadId %s and queue %s to topicThreadIdAndQueues data structure", topicThreadId, q.toString()));
            newGauge("FetchQueueSize", new Gauge<Integer>() {
                public Integer value() {
                    return q.size();
                }
            }, ImmutableMap.of("clientId", config.clientId, "topic", topicThreadId.v1, "threadId", topicThreadId.v2.threadId.toString()));
        });
        Map<String, Map<Tuple<String, ConsumerThreadId>, Tuple<LinkedBlockingQueue<FetchedDataChunk>, KafkaStream<K, V>>>> groupedByTopic = Sc.groupByKey(threadQueueStreamPairs, k -> k.v1);
        groupedByTopic.forEach((topic, m) -> {
            List<KafkaStream<K, V>> streams = Sc.map(m, (k, v) -> v.v2);
            topicStreamsMap.put(topic, streams);
            debug(String.format("adding topic %s and %d streams to map.", topic, streams.size()));
        });

        // listener to consumer and partition changes;
        zkClient.subscribeStateChanges(sessionExpirationListener);

        zkClient.subscribeChildChanges(dirs.consumerRegistryDir(), loadBalancerListener);

        topicStreamsMap.forEach((k, v) -> {
            // register on broker partition path changes;
            String topicPath = BrokerTopicsPath + "/" + k;
            zkClient.subscribeDataChanges(topicPath, topicPartitionChangeListener);
        });

        // explicitly trigger load balancing for this consumer;
        loadBalancerListener.syncedRebalance();
    }

    class WildcardStreamsHandler<K, V> implements TopicEventHandler<String> {
        public TopicFilter topicFilter;
        public Integer numStreams;
        public Decoder<K> keyDecoder;
        public Decoder<V> valueDecoder;

        public WildcardStreamsHandler(TopicFilter topicFilter, Integer numStreams, Decoder<K> keyDecoder, Decoder<V> valueDecoder) {
            this.topicFilter = topicFilter;
            this.numStreams = numStreams;
            this.keyDecoder = keyDecoder;
            this.valueDecoder = valueDecoder;
            if (messageStreamCreated.getAndSet(true))
                throw new RuntimeException("Each consumer connector can create " +
                        "message streams by filter at most once.");
            wildcardQueuesAndStreams =
                    Sc.itToList(1, numStreams, e -> {
                        LinkedBlockingQueue<FetchedDataChunk> queue = new LinkedBlockingQueue<FetchedDataChunk>(config.queuedMaxMessages);
                        KafkaStream<K, V> stream = new KafkaStream<K, V>(queue,
                                config.consumerTimeoutMs,
                                keyDecoder,
                                valueDecoder,
                                config.clientId);
                        return Tuple.of(queue, stream);
                    });
            registerConsumerInZK(dirs, consumerIdString, wildcardTopicCount);

            reinitializeConsumer(wildcardTopicCount, wildcardQueuesAndStreams);
            dirs = new ZKGroupDirs(config.groupId);
                /*
         * Topic events will trigger subsequent synced rebalances.
         */
            info("Creating topic event watcher for topics " + topicFilter);
            wildcardTopics =
                    Sc.filter(getChildrenParentMayNotExist(zkClient, BrokerTopicsPath),
                            topic -> topicFilter.isTopicAllowed(topic, config.excludeInternalTopics));
            wildcardTopicCount = TopicCount.constructTopicCount(
                    consumerIdString, topicFilter, numStreams, zkClient, config.excludeInternalTopics);
            wildcardTopicWatcher = new ZookeeperTopicEventWatcher(zkClient, this);
        }

        private List<Tuple<LinkedBlockingQueue<FetchedDataChunk>, KafkaStream<K, V>>> wildcardQueuesAndStreams;

        // bootstrap with existing topics;
        private List<String> wildcardTopics;

        private TopicCount wildcardTopicCount;

        public ZKGroupDirs dirs;


        public void handleTopicEvent(List<String> allTopics) {
            debug("Handling topic event");

            List<String> updatedTopics = Sc.filter(allTopics, topic -> topicFilter.isTopicAllowed(topic, config.excludeInternalTopics));

            List<String> addedTopics = Sc.filterNot(updatedTopics, w -> wildcardTopics.contains(w));
            if (CollectionUtils.isNotEmpty(addedTopics))
                info(String.format("Topic added event topics = %s", addedTopics));

      /*
       * Deleted TODO topics are interesting (and will not be a concern until
       * 0.8 release). We may need to remove these topics from the rebalance
       * listener's map in reinitializeConsumer.
       */
            List<String> deletedTopics = Sc.filterNot(wildcardTopics, w -> updatedTopics.contains(w));
            if (CollectionUtils.isNotEmpty(deletedTopics))
                info(String.format("Topic deleted event topics = %s", deletedTopics));

            wildcardTopics = updatedTopics;
            info(String.format("Topics to consume = %s", wildcardTopics));

            if (CollectionUtils.isNotEmpty(addedTopics) || CollectionUtils.isNotEmpty(deletedTopics))
                reinitializeConsumer(wildcardTopicCount, wildcardQueuesAndStreams);
        }

        public List<KafkaStream<K, V>> streams() {
            return Sc.map(wildcardQueuesAndStreams, s -> s.v2);
        }
    }
}
