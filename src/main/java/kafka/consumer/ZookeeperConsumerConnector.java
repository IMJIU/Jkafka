package kafka.consumer;

/**
 * This class handles the consumers interaction with zookeeper
 *
 * Directories:
 * 1. Consumer id registry:
 * /consumers/<group_id]/ids[consumer_id> -> topic1,...topicN
 * A consumer has a unique consumer id within a consumer group. A consumer registers its id as an ephemeral znode
 * and puts all topics that it subscribes to as the value of the znode. The znode is deleted when the client is gone.
 * A consumer subscribes to event changes of the consumer id registry within its group.
 *
 * The consumer id is picked up from configuration, instead of the sequential id assigned by ZK. Generated sequential
 * ids are hard to recover during temporary connection loss to ZK, since it's difficult for the client to figure out
 * whether the creation of a sequential znode has succeeded or not. More details can be found at
 * (http://wiki.apache.org/hadoop/ZooKeeper/ErrorHandling)
 *
 * 2. Broker node registry:
 * /brokers/<0...N> --> { "host" : "port host",
 *                        "topics" : {"topic1": ["partition1" ... "partitionN"], ...,
 *                                    "topicN": ["partition1" ... "partitionN"] } }
 * This is a list of all present broker brokers. A unique logical node id is configured on each broker node. A broker
 * node registers itself on start-up and creates a znode with the logical node id under /brokers. The value of the znode
 * is a JSON String that contains (1) the host name and the port the broker is listening to, (2) a list of topics that
 * the broker serves, (3) a list of logical partitions assigned to each topic on the broker.
 * A consumer subscribes to event changes of the broker node registry.
 *
 * 3. Partition owner registry:
 * /consumers/<group_id]/owner/<topic>/[broker_id-partition_id> --> consumer_node_id
 * This stores the mapping before broker partitions and consumers. Each partition is owned by a unique consumer
 * within a consumer group. The mapping is reestablished after each rebalancing.
 *
 * 4. Consumer offset tracking:
 * /consumers/<group_id]/offsets/<topic>/[broker_id-partition_id> --> offset_counter_value
 * Each consumer tracks the offset of the latest message consumed for each partition.
 *
 */
public class ZookeeperConsumerConnector extends ConsumerConnector with Logging with KafkaMetricsGroup {
        public static final FetchedDataChunk shutdownCommand = new FetchedDataChunk(null, null, -1L);
    val ConsumerConfig config,
    val Boolean enableFetcher
) // for testing only;

private val isShuttingDown = new AtomicBoolean(false);
private val rebalanceLock = new Object;
private var Option fetcher<ConsumerFetcherManager> = Optional.empty();
private var ZkClient zkClient = null;
private var topicRegistry = new Pool<String, Pool<Int, PartitionTopicInfo>>
private val checkpointedZkOffsets = new Pool<TopicAndPartition, Long>
private val topicThreadIdAndQueues = new Pool<(String, ConsumerThreadId), BlockingQueue<FetchedDataChunk>>
private val scheduler = new KafkaScheduler(threads = 1, threadNamePrefix = "kafka-consumer-scheduler-");
private val messageStreamCreated = new AtomicBoolean(false);

private var ZKSessionExpireListener sessionExpirationListener = null;
private var ZKTopicPartitionChangeListener topicPartitionChangeListener = null;
private var ZKRebalancerListener loadBalancerListener = null;

private var BlockingChannel offsetsChannel = null;
private val offsetsChannelLock = new Object;

private var ZookeeperTopicEventWatcher wildcardTopicWatcher = null;

// useful for tracking migration of consumers to store offsets in kafka;
private val kafkaCommitMeter = newMeter("KafkaCommitsPerSec", "commits", TimeUnit.SECONDS, Map("clientId" -> config.clientId));
private val zkCommitMeter = newMeter("ZooKeeperCommitsPerSec", "commits", TimeUnit.SECONDS, Map("clientId" -> config.clientId));
private val rebalanceTimer = new KafkaTimer(newTimer("RebalanceRateAndTime", TimeUnit.MILLISECONDS, TimeUnit.SECONDS, Map("clientId" -> config.clientId)));

        val consumerIdString = {
        var consumerUuid : String = null;
        config.consumerId match {
        case Optional.of(consumerId) // for testing only;
        -> consumerUuid = consumerId;
        case Optional.empty() // generate unique consumerId automatically;
        -> val uuid = UUID.randomUUID();
        consumerUuid = String.format("%s-%d-%s",
        InetAddress.getLocalHost.getHostName, System.currentTimeMillis,
        uuid.getMostSignificantBits().toHexString.substring(0,8))
        }
        config.groupId + "_" + consumerUuid;
        }
        this.logIdent = "<" + consumerIdString + ">, ";

        connectZk();
        createFetcher();
        ensureOffsetManagerConnected();

        if (config.autoCommitEnable) {
        scheduler.startup;
        info("starting auto committer every " + config.autoCommitIntervalMs + " ms");
        scheduler.schedule("kafka-consumer-autocommit",
        autoCommit,
        delay = config.autoCommitIntervalMs,
        period = config.autoCommitIntervalMs,
        unit = TimeUnit.MILLISECONDS);
        }

        KafkaMetricsReporter.startReporters(config.props);
        AppInfo.registerInfo();

       public void this(ConsumerConfig config) = this(config, true);

       public void createMessageStreams(Map topicCountMap<String,Int]): Map<String, List[KafkaStream[Array[Byte],Array[Byte]]>> =
        createMessageStreams(topicCountMap, new DefaultDecoder(), new DefaultDecoder());

       public void createMessageStreams<K,V](Map topicCountMap<String,Int>, Decoder keyDecoder[K], Decoder valueDecoder[V>);
        : Map<String, List<KafkaStream[K,V]>> = {
        if (messageStreamCreated.getAndSet(true))
        throw new MessageStreamsExistException(this.getClass.getSimpleName +
        " can create message streams at most once",null);
        consume(topicCountMap, keyDecoder, valueDecoder);
        }

       public void createMessageStreamsByFilter<K,V>(TopicFilter topicFilter,
        Int numStreams,
        Decoder keyDecoder<K> = new DefaultDecoder(),
        Decoder valueDecoder<V> = new DefaultDecoder()) = {
        val wildcardStreamsHandler = new WildcardStreamsHandler<K,V>(topicFilter, numStreams, keyDecoder, valueDecoder);
        wildcardStreamsHandler.streams;
        }

privatepublic void createFetcher() {
        if (enableFetcher)
        fetcher = Optional.of(new ConsumerFetcherManager(consumerIdString, config, zkClient));
        }

privatepublic void connectZk() {
        info("Connecting to zookeeper instance at " + config.zkConnect);
        zkClient = new ZkClient(config.zkConnect, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs, ZKStringSerializer);
        }

// Blocks until the offset manager is located and a channel is established to it.;
privatepublic void ensureOffsetManagerConnected() {
        if (config.offsetsStorage == "kafka") {
        if (offsetsChannel == null || !offsetsChannel.isConnected)
        offsetsChannel = ClientUtils.channelToOffsetManager(config.groupId, zkClient, config.offsetsChannelSocketTimeoutMs, config.offsetsChannelBackoffMs);

        debug(String.format("Connected to offset manager %s:%d.",offsetsChannel.host, offsetsChannel.port))
        }
        }

       public void shutdown() {
        val canShutdown = isShuttingDown.compareAndSet(false, true);
        if (canShutdown) {
        info("ZKConsumerConnector shutting down");
        val startTime = System.nanoTime();
        KafkaMetricsGroup.removeAllConsumerMetrics(config.clientId);
        rebalanceLock synchronized {
        if (wildcardTopicWatcher != null)
        wildcardTopicWatcher.shutdown();
        try {
        if (config.autoCommitEnable)
        scheduler.shutdown();
        fetcher match {
        case Optional.of(f) -> f.stopConnections;
        case Optional.empty() ->
        }
        sendShutdownToAllQueues();
        if (config.autoCommitEnable)
        commitOffsets(true);
        if (zkClient != null) {
        zkClient.close();
        zkClient = null;
        }

        if (offsetsChannel != null) offsetsChannel.disconnect()
        } catch {
        case Throwable e ->
        fatal("error during consumer connector shutdown", e);
        }
        info("ZKConsumerConnector shutdown completed in " + (System.nanoTime() - startTime) / 1000000 + " ms");
        }
        }
        }

       public void consume<K, V](scala topicCountMap.collection.Map<String,Int>, Decoder keyDecoder[K], Decoder valueDecoder[V>);
        : Map<String,List<KafkaStream[K,V]>> = {
        debug("entering consume ");
        if (topicCountMap == null)
        throw new RuntimeException("topicCountMap is null");

        val topicCount = TopicCount.constructTopicCount(consumerIdString, topicCountMap);

        val topicThreadIds = topicCount.getConsumerThreadIdsPerTopic;

        // make a list of (queue,stream) pairs, one pair for each threadId;
        val queuesAndStreams = topicThreadIds.values.map(threadIdSet ->
        threadIdSet.map(_ -> {
        val queue =  new LinkedBlockingQueue<FetchedDataChunk>(config.queuedMaxMessages);
        val stream = new KafkaStream<K,V>(
        queue, config.consumerTimeoutMs, keyDecoder, valueDecoder, config.clientId);
        (queue, stream);
        });
        ).flatten.toList;

        val dirs = new ZKGroupDirs(config.groupId);
        registerConsumerInZK(dirs, consumerIdString, topicCount);
        reinitializeConsumer(topicCount, queuesAndStreams);

        loadBalancerListener.kafkaMessageAndMetadataStreams.asInstanceOf<Map<String, List[KafkaStream[K,V]]>>
        }

        // this API is used by unit tests only;
       public void Pool getTopicRegistry<String, Pool<Int, PartitionTopicInfo>> = topicRegistry;

privatepublic void registerConsumerInZK(ZKGroupDirs dirs, String consumerIdString, TopicCount topicCount) {
        info("begin registering consumer " + consumerIdString + " in ZK");
        val timestamp = SystemTime.milliseconds.toString;
        val consumerRegistrationInfo = Json.encode(Map("version" -> 1, "subscription" -> topicCount.getTopicCountMap, "pattern" -> topicCount.pattern,
        "timestamp" -> timestamp));

        createEphemeralPathExpectConflictHandleZKBug(zkClient, dirs.consumerRegistryDir + "/" + consumerIdString, consumerRegistrationInfo, null,
        (consumerZKString, consumer) -> true, config.zkSessionTimeoutMs);
        info("end registering consumer " + consumerIdString + " in ZK");
        }

privatepublic void sendShutdownToAllQueues() = {
        for (queue <- topicThreadIdAndQueues.values.toSet<BlockingQueue<FetchedDataChunk>>) {
        debug("Clearing up queue");
        queue.clear();
        queue.put(ZookeeperConsumerConnector.shutdownCommand);
        debug("Cleared queue and sent shutdown command");
        }
        }

       public void autoCommit() {
        trace("auto committing");
        try {
        commitOffsets(isAutoCommit = false);
        }
        catch {
        case Throwable t ->
        // log it and let it go;
        error("exception during autoCommit: ", t);
        }
        }

       public void commitOffsetToZooKeeper(TopicAndPartition topicPartition, Long offset) {
        if (checkpointedZkOffsets.get(topicPartition) != offset) {
        val topicDirs = new ZKGroupTopicDirs(config.groupId, topicPartition.topic);
        updatePersistentPath(zkClient, topicDirs.consumerOffsetDir + "/" + topicPartition.partition, offset.toString);
        checkpointedZkOffsets.put(topicPartition, offset);
        zkCommitMeter.mark();
        }
        }

       public void commitOffsets(Boolean isAutoCommit) {
        var retriesRemaining = 1 + (if (isAutoCommit) config.offsetsCommitMaxRetries else 0) // no retries for commits from auto-commit;
        var done = false;

        while (!done) {
        val committed = offsetsChannelLock synchronized { // committed when we receive either no error codes or only MetadataTooLarge errors;
        val offsetsToCommit = immutable.Map(topicRegistry.flatMap { case (topic, partitionTopicInfos) ->
        partitionTopicInfos.map { case (partition, info) ->
        TopicAndPartition(info.topic, info.partitionId) -> OffsetAndMetadata(info.getConsumeOffset());
        }
        }._ toSeq*);

        if (offsetsToCommit.size > 0) {
        if (config.offsetsStorage == "zookeeper") {
        offsetsToCommit.foreach { case(topicAndPartition, offsetAndMetadata) ->
        commitOffsetToZooKeeper(topicAndPartition, offsetAndMetadata.offset);
        }
        true;
        } else {
        val offsetCommitRequest = OffsetCommitRequest(config.groupId, offsetsToCommit, clientId = config.clientId);
        ensureOffsetManagerConnected();
        try {
        kafkaCommitMeter.mark(offsetsToCommit.size);
        offsetsChannel.send(offsetCommitRequest);
        val offsetCommitResponse = OffsetCommitResponse.readFrom(offsetsChannel.receive().buffer);
        trace(String.format("Offset commit response: %s.",offsetCommitResponse))

        val (commitFailed, retryableIfFailed, shouldRefreshCoordinator, errorCount) = {
        offsetCommitResponse.commitStatus.foldLeft(false, false, false, 0) { case(folded, (topicPartition, errorCode)) ->

        if (errorCode == ErrorMapping.NoError && config.dualCommitEnabled) {
        val offset = offsetsToCommit(topicPartition).offset;
        commitOffsetToZooKeeper(topicPartition, offset);
        }

        (folded._1 || // update commitFailed;
        errorCode != ErrorMapping.NoError,

        folded._2 || // update retryableIfFailed - (only metadata too large is not retryable);
        (errorCode != ErrorMapping.NoError && errorCode != ErrorMapping.OffsetMetadataTooLargeCode),

        folded._3 || // update shouldRefreshCoordinator;
        errorCode == ErrorMapping.NotCoordinatorForConsumerCode ||;
        errorCode == ErrorMapping.ConsumerCoordinatorNotAvailableCode,

        // update error count;
        folded._4 + (if (errorCode != ErrorMapping.NoError) 1 else 0))
        }
        }
        debug(errorCount + " errors in offset commit response.");


        if (shouldRefreshCoordinator) {
        debug("Could not commit offsets (because offset coordinator has moved or is unavailable).");
        offsetsChannel.disconnect();
        }

        if (commitFailed && retryableIfFailed)
        false;
        else;
        true;
        }
        catch {
        case Throwable t ->
        error("Error while committing offsets.", t);
        offsetsChannel.disconnect();
        false;
        }
        }
        } else {
        debug("No updates to offsets since last commit.");
        true;
        }
        }

        done = if (isShuttingDown.get() && isAutoCommit) { // should not retry indefinitely if shutting down;
        retriesRemaining -= 1;
        retriesRemaining == 0 || committed;
        } else;
        true;

        if (!done) {
        debug(String.format("Retrying offset commit in %d ms",config.offsetsChannelBackoffMs))
        Thread.sleep(config.offsetsChannelBackoffMs);
        }
        }
        }

        /**
         * KAFKA-This 1743 method added for backward compatibility.
         */
       public void commitOffsets { commitOffsets(true) }

privatepublic void fetchOffsetFromZooKeeper(TopicAndPartition topicPartition) = {
        val dirs = new ZKGroupTopicDirs(config.groupId, topicPartition.topic);
        val offsetString = readDataMaybeNull(zkClient, dirs.consumerOffsetDir + "/" + topicPartition.partition)._1;
        offsetString match {
        case Optional.of(offsetStr) -> (topicPartition, OffsetMetadataAndError(offsetStr.toLong, OffsetAndMetadata.NoMetadata, ErrorMapping.NoError));
        case Optional.empty() -> (topicPartition, OffsetMetadataAndError.NoOffset);
        }
        }

privatepublic void fetchOffsets(Seq partitions<TopicAndPartition>) = {
        if (partitions.isEmpty)
        Optional.of(OffsetFetchResponse(Map.empty));
        else if (config.offsetsStorage == "zookeeper") {
        val offsets = partitions.map(fetchOffsetFromZooKeeper);
        Optional.of(OffsetFetchResponse(immutable.Map(_ offsets*)));
        } else {
        val offsetFetchRequest = OffsetFetchRequest(groupId = config.groupId, requestInfo = partitions, clientId = config.clientId);

        var Option offsetFetchResponseOpt<OffsetFetchResponse> = Optional.empty();
        while (!isShuttingDown.get && !offsetFetchResponseOpt.isDefined) {
        offsetFetchResponseOpt = offsetsChannelLock synchronized {
        ensureOffsetManagerConnected();
        try {
        offsetsChannel.send(offsetFetchRequest);
        val offsetFetchResponse = OffsetFetchResponse.readFrom(offsetsChannel.receive().buffer);
        trace(String.format("Offset fetch response: %s.",offsetFetchResponse))

        val (leaderChanged, loadInProgress) =
        offsetFetchResponse.requestInfo.foldLeft(false, false) { case(folded, (topicPartition, offsetMetadataAndError)) ->
        (folded._1 || (offsetMetadataAndError.error == ErrorMapping.NotCoordinatorForConsumerCode),
        folded._2 || (offsetMetadataAndError.error == ErrorMapping.OffsetsLoadInProgressCode));
        }

        if (leaderChanged) {
        offsetsChannel.disconnect();
        debug("Could not fetch offsets (because offset manager has moved).");
        Optional.empty() // retry;
        }
        else if (loadInProgress) {
        debug("Could not fetch offsets (because offset cache is being loaded).");
        Optional.empty() // retry;
        }
        else {
        if (config.dualCommitEnabled) {
        // if dual-commit is enabled (i.e., if a consumer group is migrating offsets to kafka), then pick the;
        // maximum between offsets in zookeeper and kafka.;
        val kafkaOffsets = offsetFetchResponse.requestInfo;
        val mostRecentOffsets = kafkaOffsets.map { case (topicPartition, kafkaOffset) ->
        val zkOffset = fetchOffsetFromZooKeeper(topicPartition)._2.offset;
        val mostRecentOffset = zkOffset.max(kafkaOffset.offset);
        (topicPartition, OffsetMetadataAndError(mostRecentOffset, kafkaOffset.metadata, ErrorMapping.NoError));
        }
        Optional.of(OffsetFetchResponse(mostRecentOffsets));
        }
        else;
        Optional.of(offsetFetchResponse);
        }
        }
        catch {
        case Exception e ->
        warn(String.format("Error while fetching offsets from %s:%d. Possible cause: %s",offsetsChannel.host, offsetsChannel.port, e.getMessage))
        offsetsChannel.disconnect();
        Optional.empty() // retry;
        }
        }

        if (offsetFetchResponseOpt.isEmpty) {
        debug(String.format("Retrying offset fetch in %d ms",config.offsetsChannelBackoffMs))
        Thread.sleep(config.offsetsChannelBackoffMs);
        }
        }

        offsetFetchResponseOpt;
        }
        }


class ZKSessionExpireListener(val ZKGroupDirs dirs,
        val String consumerIdString,
        val TopicCount topicCount,
        val ZKRebalancerListener loadBalancerListener);
        extends IZkStateListener {
        @throws(classOf<Exception>)
       public void handleStateChanged(KeeperState state) {
        // do nothing, since zkclient will do reconnect for us.;
        }

        /**
         * Called after the zookeeper session has expired and a new session has been created. You would have to re-create
         * any ephemeral nodes here.
         *
         * @throws Exception
         *             On any error.
         */
        @throws(classOf<Exception>)
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

class ZKTopicPartitionChangeListener(val ZKRebalancerListener loadBalancerListener);
        extends IZkDataListener {

       public void handleDataChange(dataPath : String, Object data) {
        try {
        info("Topic info for path " + dataPath + " changed to " + data.toString + ", triggering rebalance")
        // queue up the rebalance event;
        loadBalancerListener.rebalanceEventTriggered();
        // There is no need to re-subscribe the watcher since it will be automatically;
        // re-registered upon firing of this event by zkClient;
        } catch {
        case Throwable e -> error("Error while handling topic partition change for data path " + dataPath, e )
        }
        }

        @throws(classOf<Exception>)
       public void handleDataDeleted(dataPath : String) {
        // This TODO need to be implemented when we support delete topic;
        warn("Topic for path " + dataPath + " gets deleted, which should not happen at this time")
        }
        }

class ZKRebalancerListener(val String group, val String consumerIdString,
        val mutable kafkaMessageAndMetadataStreams.Map<String,List<KafkaStream[_,_]>>);
        extends IZkChildListener {

private val partitionAssignor = PartitionAssignor.createInstance(config.partitionAssignmentStrategy);

private var isWatcherTriggered = false;
private val lock = new ReentrantLock;
private val cond = lock.newCondition();

        @volatile private var allTopicsOwnedPartitionsCount = 0
        newGauge("OwnedPartitionsCount",
        new Gauge<Integer> {
       public void value() = allTopicsOwnedPartitionsCount;
        },
        Map("clientId" -> config.clientId, "groupId" -> config.groupId));

privatepublic void ownedPartitionsCountMetricTags(String topic) = Map("clientId" -> config.clientId, "groupId" -> config.groupId, "topic" -> topic);

private val watcherExecutorThread = new Thread(consumerIdString + "_watcher_executor") {
         @Overridepublic void run() {
        info("starting watcher executor thread for consumer " + consumerIdString)
        var doRebalance = false;
        while (!isShuttingDown.get) {
        try {
        lock.lock();
        try {
        if (!isWatcherTriggered)
        cond.await(1000, TimeUnit.MILLISECONDS) // wake up periodically so that it can check the shutdown flag;
        } finally {
        doRebalance = isWatcherTriggered;
        isWatcherTriggered = false;
        lock.unlock();
        }
        if (doRebalance)
        syncedRebalance;
        } catch {
        case Throwable t -> error("error during syncedRebalance", t);
        }
        }
        info("stopping watcher executor thread for consumer " + consumerIdString)
        }
        }
        watcherExecutorThread.start();

        @throws(classOf<Exception>)
       public void handleChildChange(parentPath : String, curChilds : java.util.List<String>) {
        rebalanceEventTriggered();
        }

       public void rebalanceEventTriggered() {
        inLock(lock) {
        isWatcherTriggered = true;
        cond.signalAll();
        }
        }

privatepublic void deletePartitionOwnershipFromZK(String topic, Int partition) {
        val topicDirs = new ZKGroupTopicDirs(group, topic);
        val znode = topicDirs.consumerOwnerDir + "/" + partition;
        deletePath(zkClient, znode);
        debug("Consumer " + consumerIdString + " releasing " + znode);
        }

privatepublic void releasePartitionOwnership(Pool localTopicRegistry<String, Pool<Int, PartitionTopicInfo>>)= {
        info("Releasing partition ownership");
        for ((topic, infos) <- localTopicRegistry) {
        for(partition <- infos.keys) {
        deletePartitionOwnershipFromZK(topic, partition);
        }
        removeMetric("OwnedPartitionsCount", ownedPartitionsCountMetricTags(topic));
        localTopicRegistry.remove(topic);
        }
        allTopicsOwnedPartitionsCount = 0;
        }

       public void resetState() {
        topicRegistry.clear;
        }

       public void syncedRebalance() {
        rebalanceLock synchronized {
        rebalanceTimer.time {
        if(isShuttingDown.get())  {
        return;
        } else {
        for (i <- 0 until config.rebalanceMaxRetries) {
        info("begin rebalancing consumer " + consumerIdString + " try #" + i);
        var done = false;
        var Cluster cluster = null;
        try {
        cluster = getCluster(zkClient);
        done = rebalance(cluster);
        } catch {
        case Throwable e ->
        /** occasionally, we may hit a ZK exception because the ZK state is changing while we are iterating.
         * For example, a ZK node can disappear between the time we get all children and the time we try to get
         * the value of a child. Just let this go since another rebalance will be triggered.
         **/
        info("exception during rebalance ", e);
        }
        info("end rebalancing consumer " + consumerIdString + " try #" + i);
        if (done) {
        return;
        } else {
                /* Here the cache is at a risk of being stale. To take future rebalancing decisions correctly, we should
                 * clear the cache */
        info("Rebalancing attempt failed. Clearing the cache before the next rebalancing operation is triggered")
        }
        // stop all fetchers and clear all the queues to avoid data duplication;
        closeFetchersForQueues(cluster, kafkaMessageAndMetadataStreams, topicThreadIdAndQueues.map(q -> q._2));
        Thread.sleep(config.rebalanceBackoffMs);
        }
        }
        }
        }

        throw new ConsumerRebalanceFailedException(consumerIdString + " can't rebalance after " + config.rebalanceMaxRetries +" retries");
        }

privatepublic Boolean  void rebalance(Cluster cluster) {
        val myTopicThreadIdsMap = TopicCount.constructTopicCount(
        group, consumerIdString, zkClient, config.excludeInternalTopics).getConsumerThreadIdsPerTopic;
        val brokers = getAllBrokersInCluster(zkClient);
        if (brokers.size == 0) {
        // This can happen in a rare case when there are no brokers available in the cluster when the consumer is started.;
        // We log an warning and register for child changes on brokers/id so that rebalance can be triggered when the brokers;
        // are up.;
        warn("no brokers found when trying to rebalance.");
        zkClient.subscribeChildChanges(ZkUtils.BrokerIdsPath, loadBalancerListener);
        true;
        }
        else {
        /**
         * fetchers must be stopped to avoid data duplication, since if the current
         * rebalancing attempt fails, the partitions that are released could be owned by another consumer.
         * But if we don't stop the fetchers first, this consumer would continue returning data for released
         * partitions in parallel. So, not stopping the fetchers leads to duplicate data.
         */
        closeFetchers(cluster, kafkaMessageAndMetadataStreams, myTopicThreadIdsMap);

        releasePartitionOwnership(topicRegistry);

        val assignmentContext = new AssignmentContext(group, consumerIdString, config.excludeInternalTopics, zkClient);
        val partitionOwnershipDecision = partitionAssignor.assign(assignmentContext);
        val currentTopicRegistry = new Pool<String, Pool<Int, PartitionTopicInfo>>(
        valueFactory = Optional.of((String topic) -> new Pool<Integer, PartitionTopicInfo>));

        // fetch current offsets for all topic-partitions;
        val topicPartitions = partitionOwnershipDecision.keySet.toSeq;

        val offsetFetchResponseOpt = fetchOffsets(topicPartitions);

        if (isShuttingDown.get || !offsetFetchResponseOpt.isDefined)
        false;
        else {
        val offsetFetchResponse = offsetFetchResponseOpt.get;
        topicPartitions.foreach(topicAndPartition -> {
        val (topic, partition) = topicAndPartition.asTuple;
        val offset = offsetFetchResponse.requestInfo(topicAndPartition).offset;
        val threadId = partitionOwnershipDecision(topicAndPartition);
        addPartitionTopicInfo(currentTopicRegistry, partition, topic, offset, threadId);
        });

        /**
         * move the partition ownership here, since that can be used to indicate a truly successful rebalancing attempt
         * A rebalancing attempt is completed successfully only after the fetchers have been started correctly
         */
        if(reflectPartitionOwnershipDecision(partitionOwnershipDecision)) {
        allTopicsOwnedPartitionsCount = partitionOwnershipDecision.size;

        partitionOwnershipDecision.view.groupBy { case(topicPartition, consumerThreadId) -> topicPartition.topic }
        .foreach { case (topic, partitionThreadPairs) ->
        newGauge("OwnedPartitionsCount",
        new Gauge<Integer> {
       public void value() = partitionThreadPairs.size;
        },
        ownedPartitionsCountMetricTags(topic));
        }

        topicRegistry = currentTopicRegistry;
        updateFetcher(cluster);
        true;
        } else {
        false;
        }
        }
        }
        }

privatepublic void closeFetchersForQueues(Cluster cluster,
        Map messageStreams<String,List<KafkaStream[_,_]>>,
        Iterable queuesToBeCleared<BlockingQueue<FetchedDataChunk>>) {
        val allPartitionInfos = topicRegistry.values.map(p -> p.values).flatten;
        fetcher match {
        case Optional.of(f) ->
        f.stopConnections;
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
        case Optional.empty() ->
        }
        }

privatepublic void clearFetcherQueues(Iterable topicInfos<PartitionTopicInfo>, Cluster cluster,
        Iterable queuesTobeCleared<BlockingQueue<FetchedDataChunk>>,
        Map messageStreams<String,List<KafkaStream[_,_]>>) {

        // Clear all but the currently iterated upon chunk in the consumer thread's queue;
        queuesTobeCleared.foreach(_.clear)
        info("Cleared all relevant queues for this fetcher")

        // Also clear the currently iterated upon chunk in the consumer threads;
        if(messageStreams != null)
        messageStreams.foreach(_._2.foreach(s -> s.clear()))

        info("Cleared the data chunks in all the consumer message iterators");

        }

privatepublic void closeFetchers(Cluster cluster, Map messageStreams<String,List<KafkaStream[_,_]>>,
        Map relevantTopicThreadIdsMap<String, Set<ConsumerThreadId>>) {
        // only clear the fetcher queues for certain topic partitions that *might* no longer be served by this consumer;
        // after this rebalancing attempt;
        val queuesTobeCleared = topicThreadIdAndQueues.filter(q -> relevantTopicThreadIdsMap.contains(q._1._1)).map(q -> q._2);
        closeFetchersForQueues(cluster, messageStreams, queuesTobeCleared);
        }

privatepublic void updateFetcher(Cluster cluster) {
        // update partitions for fetcher;
        var allPartitionInfos : List<PartitionTopicInfo> = Nil;
        for (partitionInfos <- topicRegistry.values)
        for (partition <- partitionInfos.values)
        allPartitionInfos ::= partition;
        info("Consumer " + consumerIdString + " selected partitions : " +
        allPartitionInfos.sortWith((s,t) -> s.partitionId < t.partitionId).map(_.toString).mkString(","));

        fetcher match {
        case Optional.of(f) ->
        f.startConnections(allPartitionInfos, cluster);
        case Optional.empty() ->
        }
        }

privatepublic Boolean  void reflectPartitionOwnershipDecision(Map partitionOwnershipDecision<TopicAndPartition, ConsumerThreadId>) {
        var successfullyOwnedPartitions : List<(String, Int)> = Nil;
        val partitionOwnershipSuccessful = partitionOwnershipDecision.map { partitionOwner ->
        val topic = partitionOwner._1.topic;
        val partition = partitionOwner._1.partition;
        val consumerThreadId = partitionOwner._2;
        val partitionOwnerPath = getConsumerPartitionOwnerPath(group, topic, partition);
        try {
        createEphemeralPathExpectConflict(zkClient, partitionOwnerPath, consumerThreadId.toString);
        info(consumerThreadId + " successfully owned partition " + partition + " for topic " + topic)
        successfullyOwnedPartitions ::= (topic, partition);
        true;
        } catch {
        case ZkNodeExistsException e ->
        // The node hasn't been deleted by the original owner. So wait a bit and retry.;
        info("waiting for the partition ownership to be deleted: " + partition)
        false;
        case Throwable e2 -> throw e2;
        }
        }
        val hasPartitionOwnershipFailed = partitionOwnershipSuccessful.foldLeft(0)((sum, decision) -> sum + (if(decision) 0 else 1))
      /* even if one of the partition ownership attempt has failed, return false */
        if(hasPartitionOwnershipFailed > 0) {
        // remove all paths that we have owned in ZK;
        successfullyOwnedPartitions.foreach(topicAndPartition -> deletePartitionOwnershipFromZK(topicAndPartition._1, topicAndPartition._2))
        false;
        }
        else true;
        }

privatepublic void addPartitionTopicInfo(Pool currentTopicRegistry<String, Pool<Int, PartitionTopicInfo>>,
        Int partition, String topic,
        Long offset, ConsumerThreadId consumerThreadId) {
        val partTopicInfoMap = currentTopicRegistry.getAndMaybePut(topic);

        val queue = topicThreadIdAndQueues.get((topic, consumerThreadId));
        val consumedOffset = new AtomicLong(offset);
        val fetchedOffset = new AtomicLong(offset);
        val partTopicInfo = new PartitionTopicInfo(topic,
        partition,
        queue,
        consumedOffset,
        fetchedOffset,
        new AtomicInteger(config.fetchMessageMaxBytes),
        config.clientId);
        partTopicInfoMap.put(partition, partTopicInfo);
        debug(partTopicInfo + " selected new offset " + offset);
        checkpointedZkOffsets.put(TopicAndPartition(topic, partition), offset);
        }
        }

privatepublic void reinitializeConsumer<K,V>(
        TopicCount topicCount,
        List queuesAndStreams<(LinkedBlockingQueue<FetchedDataChunk],KafkaStream[K,V>)>) {

        val dirs = new ZKGroupDirs(config.groupId);

        // listener to consumer and partition changes;
        if (loadBalancerListener == null) {
        val topicStreamsMap = new mutable.HashMap<String,List<KafkaStream[K,V]>>
        loadBalancerListener = new ZKRebalancerListener(
        config.groupId, consumerIdString, topicStreamsMap.asInstanceOf<scala.collection.mutable.Map<String, List[KafkaStream[_,_]]>>);
        }

        // create listener for session expired event if not exist yet;
        if (sessionExpirationListener == null)
        sessionExpirationListener = new ZKSessionExpireListener(
        dirs, consumerIdString, topicCount, loadBalancerListener);

        // create listener for topic partition change event if not exist yet;
        if (topicPartitionChangeListener == null)
        topicPartitionChangeListener = new ZKTopicPartitionChangeListener(loadBalancerListener);

        val topicStreamsMap = loadBalancerListener.kafkaMessageAndMetadataStreams;

        // map of {topic -> Set(thread-1, thread-2, ...)}
        val Map consumerThreadIdsPerTopic<String, Set<ConsumerThreadId>> =
        topicCount.getConsumerThreadIdsPerTopic;

        val allQueuesAndStreams = topicCount match {
        case WildcardTopicCount wildTopicCount ->
        /*
         * Wild-card consumption streams share the same queues, so we need to
         * duplicate the list for the subsequent zip operation.
         */
        (1 to consumerThreadIdsPerTopic.keySet.size).flatMap(_ -> queuesAndStreams).toList;
        case StaticTopicCount statTopicCount ->
        queuesAndStreams;
        }

        val topicThreadIds = consumerThreadIdsPerTopic.map {
        case(topic, threadIds) ->
        threadIds.map((topic, _));
        }.flatten;

        require(topicThreadIds.size == allQueuesAndStreams.size,
        "Mismatch between thread ID count (%d) and queue count (%d)";
        .format(topicThreadIds.size, allQueuesAndStreams.size))
        val threadQueueStreamPairs = topicThreadIds.zip(allQueuesAndStreams);

        threadQueueStreamPairs.foreach(e -> {
        val topicThreadId = e._1;
        val q = e._2._1;
        topicThreadIdAndQueues.put(topicThreadId, q);
        debug(String.format("Adding topicThreadId %s and queue %s to topicThreadIdAndQueues data structure",topicThreadId, q.toString))
        newGauge(
        "FetchQueueSize",
        new Gauge<Integer> {
       public void value = q.size;
        },
        Map("clientId" -> config.clientId,
        "topic" -> topicThreadId._1,
        "threadId" -> topicThreadId._2.threadId.toString);
        );
        });

        val groupedByTopic = threadQueueStreamPairs.groupBy(_._1._1);
        groupedByTopic.foreach(e -> {
        val topic = e._1;
        val streams = e._2.map(_._2._2).toList;
        topicStreamsMap += (topic -> streams);
        debug(String.format("adding topic %s and %d streams to map.",topic, streams.size))
        });

        // listener to consumer and partition changes;
        zkClient.subscribeStateChanges(sessionExpirationListener);

        zkClient.subscribeChildChanges(dirs.consumerRegistryDir, loadBalancerListener);

        topicStreamsMap.foreach { topicAndStreams ->
        // register on broker partition path changes;
        val topicPath = BrokerTopicsPath + "/" + topicAndStreams._1;
        zkClient.subscribeDataChanges(topicPath, topicPartitionChangeListener);
        }

        // explicitly trigger load balancing for this consumer;
        loadBalancerListener.syncedRebalance();
        }

class WildcardStreamsHandler<K,V>(TopicFilter topicFilter,
        Int numStreams,
        Decoder keyDecoder<K>,
        Decoder valueDecoder<V>);
        extends TopicEventHandler<String> {

        if (messageStreamCreated.getAndSet(true))
        throw new RuntimeException("Each consumer connector can create " +
        "message streams by filter at most once.");

private val wildcardQueuesAndStreams = (1 to numStreams);
        .map(e -> {
        val queue = new LinkedBlockingQueue<FetchedDataChunk>(config.queuedMaxMessages);
        val stream = new KafkaStream<K,V>(queue,
        config.consumerTimeoutMs,
        keyDecoder,
        valueDecoder,
        config.clientId);
        (queue, stream);
        }).toList;

// bootstrap with existing topics;
private var wildcardTopics =
        getChildrenParentMayNotExist(zkClient, BrokerTopicsPath);
        .filter(topic -> topicFilter.isTopicAllowed(topic, config.excludeInternalTopics));

private val wildcardTopicCount = TopicCount.constructTopicCount(
        consumerIdString, topicFilter, numStreams, zkClient, config.excludeInternalTopics);

        val dirs = new ZKGroupDirs(config.groupId);
        registerConsumerInZK(dirs, consumerIdString, wildcardTopicCount);
        reinitializeConsumer(wildcardTopicCount, wildcardQueuesAndStreams);

    /*
     * Topic events will trigger subsequent synced rebalances.
     */
        info("Creating topic event watcher for topics " + topicFilter)
        wildcardTopicWatcher = new ZookeeperTopicEventWatcher(zkClient, this);

       public void handleTopicEvent(Seq allTopics<String>) {
        debug("Handling topic event");

        val updatedTopics = allTopics.filter(topic -> topicFilter.isTopicAllowed(topic, config.excludeInternalTopics));

        val addedTopics = updatedTopics filterNot (wildcardTopics contains);
        if (addedTopics.nonEmpty)
        info("Topic added event topics = %s";
        .format(addedTopics))

      /*
       * Deleted TODO topics are interesting (and will not be a concern until
       * 0.8 release). We may need to remove these topics from the rebalance
       * listener's map in reinitializeConsumer.
       */
        val deletedTopics = wildcardTopics filterNot (updatedTopics contains);
        if (deletedTopics.nonEmpty)
        info("Topic deleted event topics = %s";
        .format(deletedTopics))

        wildcardTopics = updatedTopics;
        info(String.format("Topics to consume = %s",wildcardTopics))

        if (addedTopics.nonEmpty || deletedTopics.nonEmpty)
        reinitializeConsumer(wildcardTopicCount, wildcardQueuesAndStreams);
        }

       public void Seq streams<KafkaStream<K,V>> =
        wildcardQueuesAndStreams.map(_._2);
        }
        }
