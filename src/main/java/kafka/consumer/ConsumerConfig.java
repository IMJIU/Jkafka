package kafka.consumer;

import kafka.api.OffsetRequest;
import kafka.common.Config;
import kafka.common.InvalidConfigException;
import kafka.utils.Prediction;
import kafka.utils.VerifiableProperties;
import kafka.utils.ZKConfig;

import java.util.Optional;
import java.util.Properties;

/**
 * Created by Administrator on 2017/4/4.
 */
public class ConsumerConfig extends ZKConfig {//,ZKConfig
    public static final Integer RefreshMetadataBackoffMs = 200;
    public static final Integer SocketTimeout = 30 * 1000;
    public static final Integer SocketBufferSize = 64 * 1024;
    public static final Integer FetchSize = 1024 * 1024;
    public static final Integer MaxFetchSize = 10 * FetchSize;
    public static final Integer NumConsumerFetchers = 1;
    public static final Integer DefaultFetcherBackoffMs = 1000;
    public static final Boolean AutoCommit = true;
    public static final Integer AutoCommitInterval = 60 * 1000;
    public static final Integer MaxQueuedChunks = 2;
    public static final Integer MaxRebalanceRetries = 4;
    public static final String AutoOffsetReset = OffsetRequest.LargestTimeString;
    public static final Integer ConsumerTimeoutMs = -1;
    public static final Integer MinFetchBytes = 1;
    public static final Integer MaxFetchWaitMs = 100;
    public static final String MirrorTopicsWhitelist = "";
    public static final String MirrorTopicsBlacklist = "";
    public static final Integer MirrorConsumerNumThreads = 1;
    public static final Integer OffsetsChannelBackoffMs = 1000;
    public static final Integer OffsetsChannelSocketTimeoutMs = 10000;
    public static final Integer OffsetsCommitMaxRetries = 5;
    public static final String OffsetsStorage = "zookeeper";

    public static final String MirrorTopicsWhitelistProp = "mirror.topics.whitelist";
    public static final String MirrorTopicsBlacklistProp = "mirror.topics.blacklist";
    public static final Boolean ExcludeInternalTopics = true;
    public static final String DefaultPartitionAssignmentStrategy = "range"; /* select between "range", and "roundrobin" */
    public static final String MirrorConsumerNumThreadsProp = "mirror.consumer.numthreads";
    public static final String DefaultClientId = "";
    public VerifiableProperties props;

    public ConsumerConfig(VerifiableProperties props) {
        super(props);
        this.props = props;
    }

    public ConsumerConfig(Properties originalProps) {
        this(new VerifiableProperties(originalProps));
        props.verify();
    }


    /**
     * a string that uniquely identifies a set of consumers within the same consumer group
     */
    public String groupId;

    /**
     * consumer generated id automatically if not set.
     * Set this explicitly for only testing purpose.
     */
    public Optional<String> consumerId;

    /**
     * the socket timeout for network requests. Its value should be at least fetch.wait.max.ms.
     */
    public Integer socketTimeoutMs;

    /**
     * the socket receive buffer for network requests
     */
    public Integer socketReceiveBufferBytes;

    /**
     * the number of byes of messages to attempt to fetch
     */
    public Integer fetchMessageMaxBytes;

    /**
     * the number threads used to fetch data
     */
    public Integer numConsumerFetchers;

    /**
     * if true, periodically commit to zookeeper the offset of messages already fetched by the consumer
     */
    public Boolean autoCommitEnable;

    /**
     * the frequency in ms that the consumer offsets are committed to zookeeper
     */
    public Integer autoCommitIntervalMs;

    /**
     * max number of message chunks buffered for consumption, each chunk can be up to fetch.message.max.bytes
     */
    public Integer queuedMaxMessages;

    /**
     * max number of retries during rebalance
     */
    public Integer rebalanceMaxRetries;

    /**
     * the minimum amount of data the server should return for a fetch request. If insufficient data is available the request will block
     */
    public Integer fetchMinBytes;

    /**
     * the maximum amount of time the server will block before answering the fetch request if there isn't sufficient data to immediately satisfy fetch.min.bytes
     */
    public Integer fetchWaitMaxMs;

    /**
     * backoff time between retries during rebalance
     */
    public Integer rebalanceBackoffMs;

    /**
     * backoff time to refresh the leader of a partition after it loses the current leader
     */
    public Integer refreshLeaderBackoffMs;

    /**
     * backoff time to reconnect the offsets channel or to retry offset fetches/commits
     */
    public Integer offsetsChannelBackoffMs;
    /**
     * socket timeout to use when reading responses for Offset Fetch/Commit requests. This timeout will also be used for
     * the ConsumerMetdata requests that are used to query for the offset coordinator.
     */
    public Integer offsetsChannelSocketTimeoutMs;

    /**
     * Retry the offset commit up to this many times on failure. This retry count only applies to offset commits during
     * shut-down. It does not apply to commits from the auto-commit thread. It also does not apply to attempts to query
     * for the offset coordinator before committing offsets. i.e., if a consumer metadata request fails for any reason,
     * it is retried and that retry does not count toward this limit.
     */
    public Integer offsetsCommitMaxRetries;

    /**
     * Specify whether offsets should be committed to "zookeeper" (default) or "kafka"
     */
    public String offsetsStorage;

    /**
     * If you are using "kafka" as offsets.storage, you can dual commit offsets to ZooKeeper (in addition to Kafka). This
     * is required during migration from zookeeper-based offset storage to kafka-based offset storage. With respect to any
     * given consumer group, it is safe to turn this off after all instances within that group have been migrated to
     * the new jar that commits offsets to the broker (instead of directly to ZooKeeper).
     */
    public Boolean dualCommitEnabled;

    /* what to do if an offset is out of range.
       smallest : automatically reset the offset to the smallest offset
       largest : automatically reset the offset to the largest offset
       anything throw else exception to the consumer */
    public String autoOffsetReset;

    /**
     * throw a timeout exception to the consumer if no message is available for consumption after the specified interval
     */
    public Integer consumerTimeoutMs;

    /**
     * Client id is specified by the kafka consumer client, used to distinguish different clients
     */
    public String clientId;

    /**
     * Whether messages from internal topics (such as offsets) should be exposed to the consumer.
     */
    public Boolean excludeInternalTopics;

    /**
     * Select a strategy for assigning partitions to consumer streams. Possible range values, roundrobin
     */
    public String partitionAssignmentStrategy;

    public void init() {
        String groupId = props.getString("group.id");
        Optional<String> consumerId = Optional.of(props.getString("consumer.id", null));

        Integer socketTimeoutMs = props.getInt("socket.timeout.ms", SocketTimeout);
        Prediction.require(fetchWaitMaxMs <= socketTimeoutMs, "socket.timeout.ms should always be at least fetch.wait.max.ms" +
                " to prevent unnecessary socket timeouts");

        Integer socketReceiveBufferBytes = props.getInt("socket.receive.buffer.bytes", SocketBufferSize);

        Integer fetchMessageMaxBytes = props.getInt("fetch.message.max.bytes", FetchSize);
        Integer numConsumerFetchers = props.getInt("num.consumer.fetchers", NumConsumerFetchers);

        Boolean autoCommitEnable = props.getBoolean("auto.commit.enable", AutoCommit);

        Integer autoCommitIntervalMs = props.getInt("auto.commit.interval.ms", AutoCommitInterval);

        Integer queuedMaxMessages = props.getInt("queued.max.message.chunks", MaxQueuedChunks);

        Integer rebalanceMaxRetries = props.getInt("rebalance.max.retries", MaxRebalanceRetries);
        Integer fetchMinBytes = props.getInt("fetch.min.bytes", MinFetchBytes);
        Integer fetchWaitMaxMs = props.getInt("fetch.wait.max.ms", MaxFetchWaitMs);

        Integer rebalanceBackoffMs = props.getInt("rebalance.backoff.ms", zkSyncTimeMs);

        Integer refreshLeaderBackoffMs = props.getInt("refresh.leader.backoff.ms", RefreshMetadataBackoffMs);

        Integer offsetsChannelBackoffMs = props.getInt("offsets.channel.backoff.ms", OffsetsChannelBackoffMs);

        Integer offsetsChannelSocketTimeoutMs = props.getInt("offsets.channel.socket.timeout.ms", OffsetsChannelSocketTimeoutMs);

        Integer offsetsCommitMaxRetries = props.getInt("offsets.commit.max.retries", OffsetsCommitMaxRetries);

        String offsetsStorage = props.getString("offsets.storage", OffsetsStorage).toLowerCase();
        Boolean dualCommitEnabled = props.getBoolean("dual.commit.enabled", (offsetsStorage == "kafka") ? true : false);

        String autoOffsetReset = props.getString("auto.offset.reset", AutoOffsetReset);
        Integer consumerTimeoutMs = props.getInt("consumer.timeout.ms", ConsumerTimeoutMs);

        String clientId = props.getString("client.id", groupId);

        Boolean excludeInternalTopics = props.getBoolean("exclude.internal.topics", ExcludeInternalTopics);
        String partitionAssignmentStrategy = props.getString("partition.assignment.strategy", DefaultPartitionAssignmentStrategy);
        validate(this);
    }

    public static void validate(ConsumerConfig config) {
        validateClientId(config.clientId);
        validateGroupId(config.groupId);
        validateAutoOffsetReset(config.autoOffsetReset);
        validateOffsetsStorage(config.offsetsStorage);
    }

    public static void validateClientId(String clientId) {
        validateChars("client.id", clientId);
    }

    public static void validateGroupId(String groupId) {
        validateChars("group.id", groupId);
    }

    public static void validateAutoOffsetReset(String autoOffsetReset) {
        if (!OffsetRequest.SmallestTimeString.equals(autoOffsetReset)
                || !OffsetRequest.LargestTimeString.equals(autoOffsetReset)) {
            throw new InvalidConfigException("Wrong value " + autoOffsetReset + " of auto.offset.reset in ConsumerConfig; " +
                    "Valid values are " + OffsetRequest.SmallestTimeString + " and " + OffsetRequest.LargestTimeString);

        }
    }

    public static void validateOffsetsStorage(String storage) {
        if (!"zookeeper".equals(storage) || !"kafka".equals(storage)) {
            throw new InvalidConfigException("Wrong value " + storage + " of offsets.storage in consumer config; " +
                    "Valid values are 'zookeeper' and 'kafka'");
        }
    }
}
