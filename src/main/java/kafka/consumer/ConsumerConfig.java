package kafka.consumer;

import kafka.api.OffsetRequest;
import kafka.common.Config;

/**
 * Created by Administrator on 2017/4/4.
 */
public class ConsumerConfig extends Config {
    public Integer RefreshMetadataBackoffMs = 200;
    public Integer SocketTimeout = 30 * 1000;
    public Integer SocketBufferSize = 64 * 1024;
    public Integer FetchSize = 1024 * 1024;
    public Integer MaxFetchSize = 10 * FetchSize;
    public Integer NumConsumerFetchers = 1;
    public Integer DefaultFetcherBackoffMs = 1000;
    public Boolean AutoCommit = true;
    public Integer AutoCommitInterval = 60 * 1000;
    public Integer MaxQueuedChunks = 2;
    public Integer MaxRebalanceRetries = 4;
    public String AutoOffsetReset = OffsetRequest.LargestTimeString;
    public Integer ConsumerTimeoutMs = -1;
    public Integer MinFetchBytes = 1;
    public Integer MaxFetchWaitMs = 100;
    public String MirrorTopicsWhitelist = "";
    public String MirrorTopicsBlacklist = "";
    public Integer MirrorConsumerNumThreads = 1;
    public Integer OffsetsChannelBackoffMs = 1000;
    public Integer OffsetsChannelSocketTimeoutMs = 10000;
    public Integer OffsetsCommitMaxRetries = 5;
    public String OffsetsStorage = "zookeeper";

    public String MirrorTopicsWhitelistProp = "mirror.topics.whitelist";
    public String MirrorTopicsBlacklistProp = "mirror.topics.blacklist";
    public Boolean ExcludeInternalTopics = true;
    public String DefaultPartitionAssignmentStrategy = "range"; /* select between "range", and "roundrobin" */
    public String MirrorConsumerNumThreadsProp = "mirror.consumer.numthreads";
    public String DefaultClientId = "";
//
//        def validate(config: ConsumerConfig) {
//            validateClientId(config.clientId)
//            validateGroupId(config.groupId)
//            validateAutoOffsetReset(config.autoOffsetReset)
//            validateOffsetsStorage(config.offsetsStorage)
//        }
//
//        def validateClientId(clientId: String) {
//            validateChars("client.id", clientId)
//        }
//
//        def validateGroupId(groupId: String) {
//            validateChars("group.id", groupId)
//        }
//
//        def validateAutoOffsetReset(autoOffsetReset: String) {
//            autoOffsetReset match {
//                case OffsetRequest.SmallestTimeString =>
//                case OffsetRequest.LargestTimeString =>
//                case _ => throw new InvalidConfigException("Wrong value " + autoOffsetReset + " of auto.offset.reset in ConsumerConfig; " +
//                        "Valid values are " + OffsetRequest.SmallestTimeString + " and " + OffsetRequest.LargestTimeString)
//            }
//        }
//
//        def validateOffsetsStorage(storage: String) {
//            storage match {
//                case "zookeeper" =>
//                case "kafka" =>
//                case _ => throw new InvalidConfigException("Wrong value " + storage + " of offsets.storage in consumer config; " +
//                        "Valid values are 'zookeeper' and 'kafka'")
//            }
//        }
//    }
//
//    class ConsumerConfig private (val props: VerifiableProperties) extends ZKConfig(props) {
//        import ConsumerConfig._
//
//        def this(originalProps: Properties) {
//            this(new VerifiableProperties(originalProps))
//            props.verify()
//        }
//
//        /** a string that uniquely identifies a set of consumers within the same consumer group */
//        val groupId = props.getString("group.id")
//
//        /** consumer id: generated automatically if not set.
//         *  Set this explicitly for only testing purpose. */
//        val consumerId: Option[String] = Option(props.getString("consumer.id", null))
//
//        /** the socket timeout for network requests. Its value should be at least fetch.wait.max.ms. */
//        val socketTimeoutMs = props.getInt("socket.timeout.ms", SocketTimeout)
//        require(fetchWaitMaxMs <= socketTimeoutMs, "socket.timeout.ms should always be at least fetch.wait.max.ms" +
//                " to prevent unnecessary socket timeouts")
//
//        /** the socket receive buffer for network requests */
//        val socketReceiveBufferBytes = props.getInt("socket.receive.buffer.bytes", SocketBufferSize)
//
//        /** the number of byes of messages to attempt to fetch */
//        val fetchMessageMaxBytes = props.getInt("fetch.message.max.bytes", FetchSize)
//
//        /** the number threads used to fetch data */
//        val numConsumerFetchers = props.getInt("num.consumer.fetchers", NumConsumerFetchers)
//
//        /** if true, periodically commit to zookeeper the offset of messages already fetched by the consumer */
//        val autoCommitEnable = props.getBoolean("auto.commit.enable", AutoCommit)
//
//        /** the frequency in ms that the consumer offsets are committed to zookeeper */
//        val autoCommitIntervalMs = props.getInt("auto.commit.interval.ms", AutoCommitInterval)
//
//        /** max number of message chunks buffered for consumption, each chunk can be up to fetch.message.max.bytes*/
//        val queuedMaxMessages = props.getInt("queued.max.message.chunks", MaxQueuedChunks)
//
//        /** max number of retries during rebalance */
//        val rebalanceMaxRetries = props.getInt("rebalance.max.retries", MaxRebalanceRetries)
//
//        /** the minimum amount of data the server should return for a fetch request. If insufficient data is available the request will block */
//        val fetchMinBytes = props.getInt("fetch.min.bytes", MinFetchBytes)
//
//        /** the maximum amount of time the server will block before answering the fetch request if there isn't sufficient data to immediately satisfy fetch.min.bytes */
//        val fetchWaitMaxMs = props.getInt("fetch.wait.max.ms", MaxFetchWaitMs)
//
//        /** backoff time between retries during rebalance */
//        val rebalanceBackoffMs = props.getInt("rebalance.backoff.ms", zkSyncTimeMs)
//
//        /** backoff time to refresh the leader of a partition after it loses the current leader */
//        val refreshLeaderBackoffMs = props.getInt("refresh.leader.backoff.ms", RefreshMetadataBackoffMs)
//
//        /** backoff time to reconnect the offsets channel or to retry offset fetches/commits */
//        val offsetsChannelBackoffMs = props.getInt("offsets.channel.backoff.ms", OffsetsChannelBackoffMs)
//        /** socket timeout to use when reading responses for Offset Fetch/Commit requests. This timeout will also be used for
//         *  the ConsumerMetdata requests that are used to query for the offset coordinator. */
//        val offsetsChannelSocketTimeoutMs = props.getInt("offsets.channel.socket.timeout.ms", OffsetsChannelSocketTimeoutMs)
//
//        /** Retry the offset commit up to this many times on failure. This retry count only applies to offset commits during
//         * shut-down. It does not apply to commits from the auto-commit thread. It also does not apply to attempts to query
//         * for the offset coordinator before committing offsets. i.e., if a consumer metadata request fails for any reason,
//         * it is retried and that retry does not count toward this limit. */
//        val offsetsCommitMaxRetries = props.getInt("offsets.commit.max.retries", OffsetsCommitMaxRetries)
//
//        /** Specify whether offsets should be committed to "zookeeper" (default) or "kafka" */
//        val offsetsStorage = props.getString("offsets.storage", OffsetsStorage).toLowerCase
//
//        /** If you are using "kafka" as offsets.storage, you can dual commit offsets to ZooKeeper (in addition to Kafka). This
//         * is required during migration from zookeeper-based offset storage to kafka-based offset storage. With respect to any
//         * given consumer group, it is safe to turn this off after all instances within that group have been migrated to
//         * the new jar that commits offsets to the broker (instead of directly to ZooKeeper). */
//        val dualCommitEnabled = props.getBoolean("dual.commit.enabled", if (offsetsStorage == "kafka") true else false)
//
//  /* what to do if an offset is out of range.
//     smallest : automatically reset the offset to the smallest offset
//     largest : automatically reset the offset to the largest offset
//     anything else: throw exception to the consumer */
//        val autoOffsetReset = props.getString("auto.offset.reset", AutoOffsetReset)
//
//        /** throw a timeout exception to the consumer if no message is available for consumption after the specified interval */
//        val consumerTimeoutMs = props.getInt("consumer.timeout.ms", ConsumerTimeoutMs)
//
//        /**
//         * Client id is specified by the kafka consumer client, used to distinguish different clients
//         */
//        val clientId = props.getString("client.id", groupId)
//
//        /** Whether messages from internal topics (such as offsets) should be exposed to the consumer. */
//        val excludeInternalTopics = props.getBoolean("exclude.internal.topics", ExcludeInternalTopics)
//
//        /** Select a strategy for assigning partitions to consumer streams. Possible values: range, roundrobin */
//        val partitionAssignmentStrategy = props.getString("partition.assignment.strategy", DefaultPartitionAssignmentStrategy)
//
//        validate(this)
}
