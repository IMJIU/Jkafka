package kafka.producer;

import kafka.common.Config;
import kafka.common.InvalidConfigException;
import kafka.message.CompressionCodec;
import kafka.utils.Utils;
import kafka.utils.VerifiableProperties;

import java.util.List;
import java.util.Properties;

/**
 * @author zhoulf
 * @create 2017-12-01 40 14
 **/

public class ProducerConfig extends Config implements AsyncProducerConfig, SyncProducerConfigShared {
    public VerifiableProperties props;

    public ProducerConfig(VerifiableProperties props) {
        this.props = props;
        brokerList = props.getString("metadata.broker.list");
        partitionerClass = props.getString("partitioner.class", "kafka.producer.DefaultPartitioner");
        producerType = props.getString("producer.type", "sync");
        compressionCodec = props.getCompressionCodec("compression.codec", CompressionCodec.NoCompressionCodec);
        compressedTopics = Utils.parseCsvList(props.getString("compressed.topics", null));
        messageSendMaxRetries = props.getInt("message.send.max.retries", 3);
        retryBackoffMs = props.getInt("retry.backoff.ms", 100);
        topicMetadataRefreshIntervalMs = props.getInt("topic.metadata.refresh.interval.ms", 600000);
        validate(this);
    }

    public static void validate(ProducerConfig config) {
        validateClientId(config.clientId());
        validateBatchSize(config.batchNumMessages(), config.queueBufferingMaxMessages());
        validateProducerType(config.producerType);
    }

    public static void validateClientId(String clientId) {
        validateChars("client.id", clientId);
    }

    public static void validateBatchSize(Integer batchSize, Integer queueSize) {
        if (batchSize > queueSize)
            throw new InvalidConfigException("Batch size = " + batchSize + " can't be larger than queue size = " + queueSize);
    }

    public static void validateProducerType(String producerType) {
        if ("sync".equals(producerType) || "sync".equals(producerType)) {
        } else {
            throw new InvalidConfigException("Invalid value " + producerType + " for producer.type, valid values are sync/async");
        }
    }


    public ProducerConfig(Properties originalProps) {
        this(new VerifiableProperties(originalProps));
        props.verify();
    }

    /**
     * This is for bootstrapping and the producer will only use it for getting metadata
     * (topics, partitions and replicas). The socket connections for sending the actual data
     * will be established based on the broker information returned in the metadata. The
     * format is port1 host1,port2 host2, and the list can be a subset of brokers or
     * a VIP pointing to a subset of brokers.
     */
    public String brokerList;

    /**
     * the partitioner class for partitioning events amongst sub-topics
     */
    public String partitionerClass;

    /**
     * this parameter specifies whether the messages are sent asynchronously *
     * or not. Valid values are - async for asynchronous send                 *
     * sync for synchronous send
     */
    public String producerType;

    /**
     * This parameter allows you to specify the compression codec for all data generated *
     * by this producer. The default is NoCompressionCodec
     */
    public CompressionCodec compressionCodec;

    /**
     * This parameter allows you to set whether compression should be turned *
     * on for particular topics
     * <p>
     * If the compression codec is anything other than NoCompressionCodec,
     * <p>
     * Enable compression only for specified topics if any
     * <p>
     * If the list of compressed topics is empty, then enable the specified compression codec for all topics
     * <p>
     * If the compression codec is NoCompressionCodec, compression is disabled for all topics
     */
    public List<String> compressedTopics;

    /**
     * The leader may be unavailable transiently, which can fail the sending of a message.
     * This property specifies the number of retries when such failures occur.
     */
    public Integer messageSendMaxRetries;

    /**
     * Before each retry, the producer refreshes the metadata of relevant topics. Since leader
     * election takes a bit of time, this property specifies the amount of time that the producer
     * waits before refreshing the metadata.
     */
    public Integer retryBackoffMs;

    /**
     * The producer generally refreshes the topic metadata from brokers when there is a failure
     * (partition missing, leader not available...). It will also poll regularly (every default 10min
     * so 600000ms). If you set this to a negative value, metadata will only get refreshed on failure.
     * If you set this to zero, the metadata will get refreshed after each message sent (not recommended)
     * Important the note refresh happen only AFTER the message is sent, so if the producer never sends
     * a message the metadata is never refreshed
     */
    public Integer topicMetadataRefreshIntervalMs;


    @Override
    public VerifiableProperties props() {
        return props;
    }
}
