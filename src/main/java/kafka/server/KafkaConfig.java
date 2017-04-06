package kafka.server;

import kafka.consumer.ConsumerConfig;
import kafka.func.Tuple;
import kafka.message.CompressionCodec;
import kafka.message.Message;
import kafka.message.MessageSet;
import kafka.utils.*;

import java.util.*;

public class KafkaConfig extends ZKConfig {
    VerifiableProperties props;

    /**
     * Configuration settings for the kafka server
     */
    public KafkaConfig(VerifiableProperties props) {
        super(props);
        this.props = props;
        init();
        require();
    }

    public KafkaConfig(Properties originalProps) {
        this(new VerifiableProperties(originalProps));
        init();
        props.verify();
    }

    public void init() {
        /*********** General Configuration ***********/
        brokerId = props.getIntInRange("broker.id", Tuple.of(0, Integer.MAX_VALUE));
        messageMaxBytes = props.getIntInRange("message.max.bytes", 1000000 + MessageSet.LogOverhead, Tuple.of(0, Integer.MAX_VALUE));
        numNetworkThreads = props.getIntInRange("num.network.threads", 3, Tuple.of(1, Integer.MAX_VALUE));
        numIoThreads = props.getIntInRange("num.io.threads", 8, Tuple.of(1, Integer.MAX_VALUE));
        backgroundThreads = props.getIntInRange("background.threads", 10, Tuple.of(1, Integer.MAX_VALUE));
        queuedMaxRequests = props.getIntInRange("queued.max.requests", 500, Tuple.of(1, Integer.MAX_VALUE));
     /* Socket Server Configuration***********/
        port = props.getInt("port", 9092);
        hostName = props.getString("host.name", null);
        advertisedHostName = props.getString("advertised.host.name", hostName);
        advertisedPort = props.getInt("advertised.port", port);
        socketSendBufferBytes = props.getInt("socket.send.buffer.bytes", 100 * 1024);
        socketReceiveBufferBytes = props.getInt("socket.receive.buffer.bytes", 100 * 1024);
        socketRequestMaxBytes = props.getIntInRange("socket.request.max.bytes", 100 * 1024 * 1024, Tuple.of(1, Integer.MAX_VALUE));
        maxConnectionsPerIp = props.getIntInRange("max.connections.per.ip", Integer.MAX_VALUE, Tuple.of(1, Integer.MAX_VALUE));
        maxConnectionsPerIpOverrides = props.getMap("max.connections.per.ip.overrides");
        connectionsMaxIdleMs = props.getLong("connections.max.idle.ms", 10 * 60 * 1000L);
        /*********** Log Configuration ***********/
        numPartitions = props.getIntInRange("num.partitions", 1, Tuple.of(1, Integer.MAX_VALUE));
        logDirs = Utils.parseCsvList(props.getString("log.dirs", props.getString("log.dir", "/tmp/kafka-logs")));
        logSegmentBytes = props.getIntInRange("log.segment.bytes", 1 * 1024 * 1024 * 1024, Tuple.of(Message.MinHeaderSize, Integer.MAX_VALUE));
        logRollTimeMillis = getLogRollTimeMillis();
        logRollTimeJitterMillis = getLogRollTimeJitterMillis();
        logRetentionTimeMillis = getLogRetentionTimeMillis();
        logRetentionBytes = props.getLong("log.retention.bytes", -1L);
        logCleanupIntervalMs = props.getLongInRange("log.retention.check.interval.ms", 5 * 60 * 1000L, Tuple.of(1L, Long.MAX_VALUE));
        logCleanupPolicy = props.getString("log.cleanup.policy", "delete");
        logCleanerThreads = props.getIntInRange("log.cleaner.threads", 1, Tuple.of(1, Integer.MAX_VALUE));
        logCleanerIoMaxBytesPerSecond = props.getDouble("log.cleaner.io.max.bytes.per.second", Double.MAX_VALUE);
        logCleanerDedupeBufferSize = props.getLongInRange("log.cleaner.dedupe.buffer.size", 500 * 1024 * 1024L, Tuple.of(0L, Long.MAX_VALUE));
        logCleanerIoBufferSize = props.getIntInRange("log.cleaner.io.buffer.size", 512 * 1024, Tuple.of(0, Integer.MAX_VALUE));
        logCleanerDedupeBufferLoadFactor = props.getDouble("log.cleaner.io.buffer.load.factor", 0.9d);
        logCleanerBackoffMs = props.getLongInRange("log.cleaner.backoff.ms", 15 * 1000L, Tuple.of(0L, Long.MAX_VALUE));
        logCleanerMinCleanRatio = props.getDouble("log.cleaner.min.cleanable.ratio", 0.5);
        logCleanerEnable = props.getBoolean("log.cleaner.enable", false);
        logCleanerDeleteRetentionMs = props.getLong("log.cleaner.delete.retention.ms", 24 * 60 * 60 * 1000L);
        logIndexSizeMaxBytes = props.getIntInRange("log.index.size.max.bytes", 10 * 1024 * 1024, Tuple.of(4, Integer.MAX_VALUE));
        logIndexIntervalBytes = props.getIntInRange("log.index.interval.bytes", 4096, Tuple.of(0, Integer.MAX_VALUE));
        logFlushIntervalMessages = props.getLongInRange("log.flush.interval.messages", Long.MAX_VALUE, Tuple.of(1L, Long.MAX_VALUE));
        logDeleteDelayMs = props.getLongInRange("log.segment.delete.delay.ms", 60000L, Tuple.of(0L, Long.MAX_VALUE));
        logFlushSchedulerIntervalMs = props.getLong("log.flush.scheduler.interval.ms", Long.MAX_VALUE);
        logFlushIntervalMs = props.getLong("log.flush.interval.ms", logFlushSchedulerIntervalMs);
        logFlushOffsetCheckpointIntervalMs = props.getIntInRange("log.flush.offset.checkpoint.interval.ms", 60000, Tuple.of(0, Integer.MAX_VALUE));
        numRecoveryThreadsPerDataDir = props.getIntInRange("num.recovery.threads.per.data.dir", 1, Tuple.of(1, Integer.MAX_VALUE));
        autoCreateTopicsEnable = props.getBoolean("auto.create.topics.enable", true);
        minInSyncReplicas = props.getIntInRange("min.insync.replicas", 1, Tuple.of(1, Integer.MAX_VALUE));
    /* Replication configuration***********/
        controllerSocketTimeoutMs = props.getInt("controller.socket.timeout.ms", 30000);
        controllerMessageQueueSize = props.getInt("controller.message.queue.size", Integer.MAX_VALUE);
        defaultReplicationFactor = props.getInt("default.replication.factor", 1);
        replicaLagTimeMaxMs = props.getLong("replica.lag.time.max.ms", 10000L);
        replicaLagMaxMessages = props.getLong("replica.lag.max.messages", 4000L);
        replicaSocketTimeoutMs = props.getInt("replica.socket.timeout.ms", ConsumerConfig.SocketTimeout);
        replicaSocketReceiveBufferBytes = props.getInt("replica.socket.receive.buffer.bytes", ConsumerConfig.SocketBufferSize);
        replicaFetchMaxBytes = props.getIntInRange("replica.fetch.max.bytes", ConsumerConfig.FetchSize, Tuple.of(messageMaxBytes, Integer.MAX_VALUE));

        replicaFetchWaitMaxMs = props.getInt("replica.fetch.wait.max.ms", 500);
        replicaFetchMinBytes = props.getInt("replica.fetch.min.bytes", 1);
        numReplicaFetchers = props.getInt("num.replica.fetchers", 1);
        replicaHighWatermarkCheckpointIntervalMs = props.getLong("replica.high.watermark.checkpoint.interval.ms", 5000L);
        fetchPurgatoryPurgeIntervalRequests = props.getInt("fetch.purgatory.purge.interval.requests", 1000);
        producerPurgatoryPurgeIntervalRequests = props.getInt("producer.purgatory.purge.interval.requests", 1000);
        autoLeaderRebalanceEnable = props.getBoolean("auto.leader.rebalance.enable", true);
        leaderImbalancePerBrokerPercentage = props.getInt("leader.imbalance.per.broker.percentage", 10);
        leaderImbalanceCheckIntervalSeconds = props.getInt("leader.imbalance.check.interval.seconds", 300);
        uncleanLeaderElectionEnable = props.getBoolean("unclean.leader.election.enable", true);
        /*********** Controlled shutdown configuration ***********/
        controlledShutdownMaxRetries = props.getInt("controlled.shutdown.max.retries", 3);
        controlledShutdownRetryBackoffMs = props.getInt("controlled.shutdown.retry.backoff.ms", 5000);
        controlledShutdownEnable = props.getBoolean("controlled.shutdown.enable", true);
     /* Offset management configuration ***********/
        offsetMetadataMaxSize = props.getInt("offset.metadata.max.bytes", OffsetManagerConfig.DefaultMaxMetadataSize);
        offsetsLoadBufferSize = props.getIntInRange("offsets.load.buffer.size", OffsetManagerConfig.DefaultLoadBufferSize, Tuple.of(1, Integer.MAX_VALUE));

        offsetsTopicReplicationFactor = props.getShortInRange("offsets.topic.replication.factor",
                OffsetManagerConfig.DefaultOffsetsTopicReplicationFactor, Tuple.of((short) 1, Short.MAX_VALUE));
        offsetsTopicPartitions = props.getIntInRange("offsets.topic.num.partitions",
                OffsetManagerConfig.DefaultOffsetsTopicNumPartitions, Tuple.of(1, Integer.MAX_VALUE));
        offsetsTopicSegmentBytes = props.getIntInRange("offsets.topic.segment.bytes",
                OffsetManagerConfig.DefaultOffsetsTopicSegmentBytes, Tuple.of(1, Integer.MAX_VALUE));
        offsetsTopicCompressionCodec = props.getCompressionCodec("offsets.topic.compression.codec",
                OffsetManagerConfig.DefaultOffsetsTopicCompressionCodec);
        offsetsRetentionMinutes = props.getIntInRange("offsets.retention.minutes", 24 * 60, Tuple.of(1, Integer.MAX_VALUE));
        offsetsRetentionCheckIntervalMs = props.getLongInRange("offsets.retention.check.interval.ms",
                OffsetManagerConfig.DefaultOffsetsRetentionCheckIntervalMs, Tuple.of(1L, Long.MAX_VALUE));
        offsetCommitTimeoutMs = props.getIntInRange("offsets.commit.timeout.ms",
                OffsetManagerConfig.DefaultOffsetCommitTimeoutMs, Tuple.of(1, Integer.MAX_VALUE));
        offsetCommitRequiredAcks = props.getShortInRange("offsets.commit.required.acks",
                OffsetManagerConfig.DefaultOffsetCommitRequiredAcks, Tuple.of((short) -1, offsetsTopicReplicationFactor));
        deleteTopicEnable = props.getBoolean("delete.topic.enable", false);
    }

    public void require() {
        Prediction.require(logDirs.size() > 0);
        Prediction.require(logCleanerDedupeBufferSize / logCleanerThreads > 1024 * 1024, "log.cleaner.dedupe.buffer.size must be at least 1MB per cleaner thread.");
        Prediction.require(replicaFetchWaitMaxMs <= replicaSocketTimeoutMs, "replica.socket.timeout.ms should always be at least replica.fetch.wait.max.ms" +
                " to prevent unnecessary socket timeouts");
        Prediction.require(replicaFetchWaitMaxMs <= replicaLagTimeMaxMs, "replica.fetch.wait.max.ms should always be at least replica.lag.time.max.ms" +
                " to prevent frequent changes in ISR");
    }

    private Long getLogRetentionTimeMillis() {
        Long millisInMinute = 60L * 1000L;
        Long millisInHour = 60L * millisInMinute;

        if (props.containsKey("log.retention.ms")) {
            return props.getIntInRange("log.retention.ms", Tuple.of(1, Integer.MAX_VALUE)).longValue();
        } else if (props.containsKey("log.retention.minutes")) {
            return millisInMinute * props.getIntInRange("log.retention.minutes", Tuple.of(1, Integer.MAX_VALUE));
        } else {
            return millisInHour * props.getIntInRange("log.retention.hours", 24 * 7, Tuple.of(1, Integer.MAX_VALUE));
        }
    }

    private Long getLogRollTimeMillis() {
        Long millisInHour = 60L * 60L * 1000L;
        if (props.containsKey("log.roll.ms")) {
            return props.getIntInRange("log.roll.ms", Tuple.of(1, Integer.MAX_VALUE)).longValue();
        } else {
            return millisInHour * props.getIntInRange("log.roll.hours", 24 * 7, Tuple.of(1, Integer.MAX_VALUE));
        }
    }

    private Long getLogRollTimeJitterMillis() {
        Long millisInHour = 60L * 60L * 1000L;

        if (props.containsKey("log.roll.jitter.ms")) {
            return props.getIntInRange("log.roll.jitter.ms", Tuple.of(0, Integer.MAX_VALUE)).longValue();
        } else {
            return millisInHour * props.getIntInRange("log.roll.jitter.hours", 0, Tuple.of(0, Integer.MAX_VALUE));
        }
    }

    /*********** General Configuration ***********/

    /* the broker id for this server */
    public Integer brokerId;
    /* the maximum size of message that the server can receive */
    public Integer messageMaxBytes ;

    /* the number of network threads that the server uses for handling network requests */
    public Integer numNetworkThreads ;

    /* the number of io threads that the server uses for carrying out network requests */
    public Integer numIoThreads ;

    /* the number of threads to use for various background processing tasks */
    public Integer backgroundThreads ;

    /* the number of queued requests allowed before blocking the network threads */
    public Integer queuedMaxRequests ;

    /***********
     * Socket Server Configuration
     ***********/

    /* the port to listen and accept connections on */
    public Integer port ;

    /* hostname of broker. If this is set, it will only bind to this address. If this is not set,
     * it will bind to all interfaces */
    public String hostName ;

    /* hostname to publish to ZooKeeper for clients to use. In IaaS environments, this may
     * need to be different from the interface to which the broker binds. If this is not set,
     * it will use the value for "host.name" if configured. Otherwise
     * it will use the value returned from java.net.InetAddress.getCanonicalHostName(). */
    public String advertisedHostName ;

    /* the port to publish to ZooKeeper for clients to use. In IaaS environments, this may
     * need to be different from the port to which the broker binds. If this is not set,
     * it will publish the same port that the broker binds to. */
    public Integer advertisedPort ;

    /* the SO_SNDBUFF buffer of the socket sever sockets */
    public Integer socketSendBufferBytes ;

    /* the SO_RCVBUFF buffer of the socket sever sockets */
    public Integer socketReceiveBufferBytes ;

    /* the maximum number of bytes in a socket request */
    public Integer socketRequestMaxBytes ;

    /* the maximum number of connections we allow from each ip address */
    public Integer maxConnectionsPerIp ;

    /* per-ip or hostname overrides to the default maximum number of connections */
    public Map<String, String> maxConnectionsPerIpOverrides ;

    /* idle connections the timeout server socket processor threads close the connections that idle more than this */
    public Long connectionsMaxIdleMs ;

    /*********** Log Configuration ***********/

    /* the default number of log partitions per topic */
    public Integer numPartitions ;

    /* the directories in which the log data is kept */
    public List<String> logDirs ;


    /* the maximum size of a single log file */
    public Integer logSegmentBytes ;

    /* the maximum time before a new log segment is rolled out */
    public Long logRollTimeMillis ;

    /* the maximum jitter to subtract from logRollTimeMillis */
    public Long logRollTimeJitterMillis ;

    /* the number of hours to keep a log file before deleting it */
    public Long logRetentionTimeMillis ;

    /* the maximum size of the log before deleting it */
    public Long logRetentionBytes ;

    /* the frequency in minutes that the log cleaner checks whether any log is eligible for deletion */
    public Long logCleanupIntervalMs ;

    /* the default cleanup policy for segments beyond the retention window, must be either "delete" or "compact" */
    public String logCleanupPolicy ;

    /* the number of background threads to use for log cleaning */
    public Integer logCleanerThreads ;

    /* the log cleaner will be throttled so that the sum of its read and write i/o will be less than this value on average */
    public Double logCleanerIoMaxBytesPerSecond ;

    /* the total memory used for log deduplication across all cleaner threads */
    public Long logCleanerDedupeBufferSize ;


    /* the total memory used for log cleaner I/O buffers across all cleaner threads */
    public Integer logCleanerIoBufferSize ;

    /* log cleaner dedupe buffer load factor. The percentage full the dedupe buffer can become. A higher value
     * will allow more log to be cleaned at once but will lead to more hash collisions */
    public Double logCleanerDedupeBufferLoadFactor ;

    /* the amount of time to sleep when there are no logs to clean */
    public Long logCleanerBackoffMs ;

    /* the minimum ratio of dirty log to total log for a log to eligible for cleaning */
    public Double logCleanerMinCleanRatio ;

    /* should we enable log cleaning? */
    public Boolean logCleanerEnable ;

    /* how long are delete records retained? */
    public Long logCleanerDeleteRetentionMs ;

    /* the maximum size in bytes of the offset index */
    public Integer logIndexSizeMaxBytes ;

    /* the interval with which we add an entry to the offset index */
    public Integer logIndexIntervalBytes ;

    /* the number of messages accumulated on a log partition before messages are flushed to disk */
    public Long logFlushIntervalMessages ;

    /* the amount of time to wait before deleting a file from the filesystem */
    public Long logDeleteDelayMs ;

    /* the frequency in ms that the log flusher checks whether any log needs to be flushed to disk */
    public Long logFlushSchedulerIntervalMs ;

    /* the maximum time in ms that a message in any topic is kept in memory before flushed to disk */
    public Long logFlushIntervalMs ;

    /* the frequency with which we update the persistent record of the last flush which acts as the log recovery point */
    public Integer logFlushOffsetCheckpointIntervalMs ;

    /* the number of threads per data directory to be used for log recovery at startup and flushing at shutdown */
    public Integer numRecoveryThreadsPerDataDir ;

    /* enable auto creation of topic on the server */
    public Boolean autoCreateTopicsEnable ;

    /* define the minimum number of replicas in ISR needed to satisfy a produce request with required.acks=-1 (or all) */
    public Integer minInSyncReplicas ;


    /***********
     * Replication configuration
     ***********/

  /* the socket timeout for controller-to-broker channels */
    public Integer controllerSocketTimeoutMs ;

    /* the buffer size for controller-to-broker-channels */
    public Integer controllerMessageQueueSize ;

    /* default replication factors for automatically created topics */
    public Integer defaultReplicationFactor ;

    /* If a follower hasn't sent any fetch requests during this time, the leader will remove the follower from isr */
    public Long replicaLagTimeMaxMs ;

    /* If the lag in messages between a leader and a follower exceeds this number, the leader will remove the follower from isr */
    public Long replicaLagMaxMessages ;

    /* the socket timeout for network requests. Its value should be at least replica.fetch.wait.max.ms. */
    public Integer replicaSocketTimeoutMs ;

    /* the socket receive buffer for network requests */
    public Integer replicaSocketReceiveBufferBytes ;

    /* the number of byes of messages to attempt to fetch */
    public Integer replicaFetchMaxBytes ;

    /* max wait time for each fetcher request issued by follower replicas. This value should always be less than the
    *  replica.lag.time.max.ms at all times to prevent frequent shrinking of ISR for low throughput topics */
    public Integer replicaFetchWaitMaxMs ;

    /* minimum bytes expected for each fetch response. If not enough bytes, wait up to replicaMaxWaitTimeMs */
    public Integer replicaFetchMinBytes ;

    /* number of fetcher threads used to replicate messages from a source broker.
     * Increasing this value can increase the degree of I/O parallelism in the follower broker. */
    public Integer numReplicaFetchers ;

    /* the frequency with which the high watermark is saved out to disk */
    public Long replicaHighWatermarkCheckpointIntervalMs ;

    /* the purge interval (in number of requests) of the fetch request purgatory */
    public Integer fetchPurgatoryPurgeIntervalRequests ;

    /* the purge interval (in number of requests) of the producer request purgatory */
    public Integer producerPurgatoryPurgeIntervalRequests ;

    /* Enables auto leader balancing. A background thread checks and triggers leader
     * balance if required at regular intervals */
    public Boolean autoLeaderRebalanceEnable ;

    /* the ratio of leader imbalance allowed per broker. The controller would trigger a leader balance if it goes above
     * this value per broker. The value is specified in percentage. */
    public Integer leaderImbalancePerBrokerPercentage ;

    /* the frequency with which the partition rebalance check is triggered by the controller */
    public Integer leaderImbalanceCheckIntervalSeconds ;

    /* indicates whether to enable replicas not in the ISR set to be elected as leader as a last resort, even though
     * doing so may result in data loss */
    public Boolean uncleanLeaderElectionEnable ;

    /*********** Controlled shutdown configuration ***********/

    /**
     * Controlled shutdown can fail for multiple reasons. This determines the number of retries when such failure happens
     */
    public Integer controlledShutdownMaxRetries ;

    /**
     * Before each retry, the system needs time to recover from the state that caused the previous failure (Controller
     * fail over, replica lag etc). This config determines the amount of time to wait before retrying.
     */
    public Integer controlledShutdownRetryBackoffMs ;

    /* enable controlled shutdown of the server */
    public Boolean controlledShutdownEnable ;

    /***********
     * Offset management configuration
     ***********/

  /* the maximum size for a metadata entry associated with an offset commit */
    public Integer offsetMetadataMaxSize ;

    /**
     * Batch size for reading from the offsets segments when loading offsets into the cache.
     */
    public Integer offsetsLoadBufferSize ;

    /**
     * The replication factor for the offsets topic (set higher to ensure availability). To
     * ensure that the effective replication factor of the offsets topic is the configured value,
     * the number of alive brokers has to be at least the replication factor at the time of the
     * first request for the offsets topic. If not, either the offsets topic creation will fail or
     * it will get a replication factor of min(alive brokers, configured replication factor)
     */
    public Short offsetsTopicReplicationFactor ;

    /**
     * The number of partitions for the offset commit topic (should not change after deployment).
     */
    public Integer offsetsTopicPartitions ;

    /**
     * The offsets topic segment bytes should be kept relatively small in order to facilitate faster log compaction and cache loads
     */
    public Integer offsetsTopicSegmentBytes;

    /**
     * Compression codec for the offsets topic - compression may be used to achieve "atomic" commits.
     */
    public CompressionCodec offsetsTopicCompressionCodec ;

    /**
     * Offsets older than this retention period will be discarded.
     */
    public Integer offsetsRetentionMinutes ;

    /**
     * Frequency at which to check for stale offsets.
     */
    public Long offsetsRetentionCheckIntervalMs;

    /* Offset commit will be delayed until all replicas for the offsets topic receive the commit or this timeout is
     * reached. This is similar to the producer request timeout. */
    public Integer offsetCommitTimeoutMs ;

    /**
     * The required acks before the commit can be accepted. In general, the default (-1) should not be overridden.
     */
    public Short offsetCommitRequiredAcks ;

    /* Enables delete topic. Delete topic through the admin tool will have no effect if this config is turned off */
    public Boolean deleteTopicEnable ;

}

