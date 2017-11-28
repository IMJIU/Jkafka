package kafka.server;

import kafka.message.CompressionCodec;
import kafka.metrics.KafkaMetricsGroup;

/**
 * Created by Administrator on 2017/4/4.
 */
public class OffsetManagerConfig extends KafkaMetricsGroup {
    public static final Integer DefaultMaxMetadataSize = 4096;
    public static final Integer DefaultLoadBufferSize = 5 * 1024 * 1024;
    public static final Long DefaultOffsetsRetentionCheckIntervalMs = 600000L;
    public static final Integer DefaultOffsetsTopicNumPartitions = 50;
    public static final Integer DefaultOffsetsTopicSegmentBytes = 100 * 1024 * 1024;
    public static final Short DefaultOffsetsTopicReplicationFactor = 3;
    public static final CompressionCodec DefaultOffsetsTopicCompressionCodec = CompressionCodec.NoCompressionCodec;
    public static final Integer DefaultOffsetCommitTimeoutMs = 5000;
    public static final Short DefaultOffsetCommitRequiredAcks = (-1);

    public Integer maxMetadataSize = OffsetManagerConfig.DefaultMaxMetadataSize;
    public Integer loadBufferSize = OffsetManagerConfig.DefaultLoadBufferSize;
    public Long offsetsRetentionMs = 24 * 60 * 60000L;
    public Long offsetsRetentionCheckIntervalMs = OffsetManagerConfig.DefaultOffsetsRetentionCheckIntervalMs;
    public Integer offsetsTopicNumPartitions = OffsetManagerConfig.DefaultOffsetsTopicNumPartitions;
    public Integer offsetsTopicSegmentBytes = OffsetManagerConfig.DefaultOffsetsTopicSegmentBytes;
    public Short offsetsTopicReplicationFactor = OffsetManagerConfig.DefaultOffsetsTopicReplicationFactor;
    public CompressionCodec offsetsTopicCompressionCodec = OffsetManagerConfig.DefaultOffsetsTopicCompressionCodec;
    public Integer offsetCommitTimeoutMs = OffsetManagerConfig.DefaultOffsetCommitTimeoutMs;
    public Short offsetCommitRequiredAcks = OffsetManagerConfig.DefaultOffsetCommitRequiredAcks;

    public OffsetManagerConfig(Integer maxMetadataSize, Integer loadBufferSize, Long offsetsRetentionMs, Long offsetsRetentionCheckIntervalMs, Integer offsetsTopicNumPartitions, Integer offsetsTopicSegmentBytes, Short offsetsTopicReplicationFactor, CompressionCodec offsetsTopicCompressionCodec, Integer offsetCommitTimeoutMs, Short offsetCommitRequiredAcks) {
        this.maxMetadataSize = maxMetadataSize == null ? OffsetManagerConfig.DefaultMaxMetadataSize : maxMetadataSize;
        this.loadBufferSize = loadBufferSize == null ? OffsetManagerConfig.DefaultLoadBufferSize : loadBufferSize;
        this.offsetsRetentionMs = offsetsRetentionMs == null ? 24 * 60 * 60000L : offsetsRetentionMs;
        this.offsetsRetentionCheckIntervalMs = offsetsRetentionCheckIntervalMs == null ? OffsetManagerConfig.DefaultOffsetsRetentionCheckIntervalMs : offsetsRetentionCheckIntervalMs;
        this.offsetsTopicNumPartitions = offsetsTopicNumPartitions == null ? OffsetManagerConfig.DefaultOffsetsTopicNumPartitions : offsetsTopicNumPartitions;
        this.offsetsTopicSegmentBytes = offsetsTopicSegmentBytes == null ? OffsetManagerConfig.DefaultOffsetsTopicSegmentBytes : offsetsTopicSegmentBytes;
        this.offsetsTopicReplicationFactor = offsetsTopicReplicationFactor == null ? OffsetManagerConfig.DefaultOffsetsTopicReplicationFactor : offsetsTopicReplicationFactor;
        this.offsetsTopicCompressionCodec = offsetsTopicCompressionCodec == null ? OffsetManagerConfig.DefaultOffsetsTopicCompressionCodec : offsetsTopicCompressionCodec;
        this.offsetCommitTimeoutMs = offsetCommitTimeoutMs == null ? OffsetManagerConfig.DefaultOffsetCommitTimeoutMs : offsetCommitTimeoutMs;
        this.offsetCommitRequiredAcks = offsetCommitRequiredAcks == null ? OffsetManagerConfig.DefaultOffsetCommitRequiredAcks : offsetCommitRequiredAcks;
    }
/**
 * Configuration settings for in-built offset management
 *
 * @param maxMetadataSize The maximum allowed metadata for any offset commit.
 * @param loadBufferSize Batch size for reading from the offsets segments when loading offsets into the cache.
 * @param offsetsRetentionMs Offsets older than this retention period will be discarded.
 * @param offsetsRetentionCheckIntervalMs Frequency at which to check for stale offsets.
 * @param offsetsTopicNumPartitions The number of partitions for the offset commit topic (should not change after deployment).
 * @param offsetsTopicSegmentBytes The offsets topic segment bytes should be kept relatively small to facilitate faster
 * log compaction and faster offset loads
 * @param offsetsTopicReplicationFactor The replication factor for the offset commit topic (set higher to ensure availability).
 * @param offsetsTopicCompressionCodec Compression codec for the offsets topic - compression should be turned on in
 * order to achieve "atomic" commits.
 * @param offsetCommitTimeoutMs The offset commit will be delayed until all replicas for the offsets topic receive the
 * commit or this timeout is reached. (Similar to the producer request timeout.)
 * @param offsetCommitRequiredAcks The required acks before the commit can be accepted. In general, the default (-1)
 * should not be overridden.
 */
}