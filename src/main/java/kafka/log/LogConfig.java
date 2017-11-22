package kafka.log;/**
 * Created by zhoulf on 2017/4/1.
 */

import com.google.common.collect.Sets;
import kafka.common.InvalidConfigException;
import kafka.utils.Prediction;

import java.util.Properties;
import java.util.Random;
import java.util.Set;

/**
 * @author
 * @create 2017-04-01 30 22
 **/
public class LogConfig implements Cloneable{
    public Integer segmentSize;
    public Long segmentMs;
    public Long segmentJitterMs;
    public Long flushInterval;
    public Long flushMs;
    public Long retentionSize;
    public Long retentionMs;
    public Integer maxMessageSize;
    public Integer maxIndexSize;
    public Integer indexInterval;
    public Long fileDeleteDelayMs;
    public Long deleteRetentionMs;
    public Double minCleanableRatio;
    public Boolean compact;
    public Boolean uncleanLeaderElectionEnable;
    public Integer minInSyncReplicas;



    interface Defaults {
        Integer SegmentSize = 1024 * 1024;
        Long SegmentMs = Long.MAX_VALUE;
        Long SegmentJitterMs = 0L;
        Long FlushInterval = Long.MAX_VALUE;
        Long FlushMs = Long.MAX_VALUE;
        Long RetentionSize = Long.MAX_VALUE;
        Long RetentionMs = Long.MAX_VALUE;
        Integer MaxMessageSize = Integer.MAX_VALUE;
        Integer MaxIndexSize = 1024 * 1024;
        Integer IndexInterval = 4096;
        Long FileDeleteDelayMs = 60 * 1000L;
        Long DeleteRetentionMs = 24 * 60 * 60 * 1000L;
        Double MinCleanableDirtyRatio = 0.5;
        Boolean Compact = false;
        Boolean UncleanLeaderElectionEnable = true;
        Integer MinInSyncReplicas = 1;
    }

    /**
     * Configuration settings for a log
     *
     * @param segmentSize                 The soft maximum for the size of a segment file in the log
     * @param segmentMs                   The soft maximum on the amount of time before a new log segment is rolled
     * @param segmentJitterMs             The maximum random jitter subtracted from segmentMs to avoid thundering herds of segment rolling
     * @param flushInterval               The number of messages that can be written to the log before a flush is forced
     * @param flushMs                     The amount of time the log can have dirty data before a flush is forced
     * @param retentionSize               The approximate total number of bytes this log can use
     * @param retentionMs                 The age approximate maximum age of the last segment that is retained
     * @param maxIndexSize                The maximum size of an index file
     * @param indexInterval               The approximate number of bytes between index entries
     * @param fileDeleteDelayMs           The time to wait before deleting a file from the filesystem
     * @param deleteRetentionMs           The time to retain delete markers in the log. Only applicable for logs that are being compacted.
     * @param minCleanableRatio           The ratio of bytes that are available for cleaning to the bytes already cleaned
     * @param compact                     Should old segments in this log be deleted or deduplicated?
     * @param uncleanLeaderElectionEnable Indicates whether unclean leader election is enabled; actually a controller-level property
     *                                    but included here for topic-specific configuration validation purposes
     * @param minInSyncReplicas           If number of insync replicas drops below this number, we stop accepting writes with -1 (or all) required acks
     */
    public LogConfig(Integer segmentSize,
                     Long segmentMs,
                     Long segmentJitterMs,
                     Long flushInterval,
                     Long flushMs,
                     Long retentionSize,
                     Long retentionMs,
                     Integer maxMessageSize,
                     Integer maxIndexSize,
                     Integer indexInterval,
                     Long fileDeleteDelayMs,
                     Long deleteRetentionMs,
                     Double minCleanableRatio,
                     Boolean compact,
                     Boolean uncleanLeaderElectionEnable,
                     Integer minInSyncReplicas) {
        this.segmentSize = segmentSize == null ? Defaults.SegmentSize : segmentSize;
        this.segmentMs = segmentMs == null ? Defaults.SegmentMs : segmentMs;
        this.segmentJitterMs = segmentJitterMs == null ? Defaults.SegmentJitterMs : segmentJitterMs;
        this.flushInterval = flushInterval == null ? Defaults.FlushInterval : flushInterval;
        this.flushMs = flushMs == null ? Defaults.FlushMs : flushMs;
        this.retentionSize = retentionSize == null ? Defaults.RetentionSize : retentionSize;
        this.retentionMs = retentionMs == null ? Defaults.RetentionMs : retentionMs;
        this.maxMessageSize = maxMessageSize == null ? Defaults.MaxMessageSize : maxMessageSize;
        this.maxIndexSize = maxIndexSize == null ? Defaults.MaxIndexSize : maxIndexSize;
        this.indexInterval = indexInterval == null ? Defaults.IndexInterval : indexInterval;
        this.fileDeleteDelayMs = fileDeleteDelayMs == null ? Defaults.FileDeleteDelayMs : fileDeleteDelayMs;
        this.deleteRetentionMs = deleteRetentionMs == null ? Defaults.DeleteRetentionMs : deleteRetentionMs;
        this.minCleanableRatio = minCleanableRatio == null ? Defaults.MinCleanableDirtyRatio : minCleanableRatio;
        this.compact = compact == null ? Defaults.Compact : compact;
        this.uncleanLeaderElectionEnable = uncleanLeaderElectionEnable == null ? Defaults.UncleanLeaderElectionEnable : uncleanLeaderElectionEnable;
        this.minInSyncReplicas = minInSyncReplicas == null ? Defaults.MinInSyncReplicas : minInSyncReplicas;
        init();
    }

    public LogConfig() {
        this(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
    }

    /**
     * Parse the given properties instance into a LogConfig object
     */
    public static LogConfig fromProps(Properties props) {
        return new LogConfig(new Integer(props.getProperty(SegmentBytesProp, Defaults.SegmentSize.toString())),
                new Long(props.getProperty(SegmentMsProp, Defaults.SegmentMs.toString())),
                new Long(props.getProperty(SegmentJitterMsProp, Defaults.SegmentJitterMs.toString())),
                new Long(props.getProperty(FlushMessagesProp, Defaults.FlushInterval.toString())),
                new Long(props.getProperty(FlushMsProp, Defaults.FlushMs.toString())),
                new Long(props.getProperty(RetentionBytesProp, Defaults.RetentionSize.toString())),
                new Long(props.getProperty(RententionMsProp, Defaults.RetentionMs.toString())),
                new Integer(props.getProperty(MaxMessageBytesProp, Defaults.MaxMessageSize.toString())), null,
                new Integer(props.getProperty(IndexIntervalBytesProp, Defaults.IndexInterval.toString())),
                new Long(props.getProperty(FileDeleteDelayMsProp, Defaults.FileDeleteDelayMs.toString())),
                new Long(props.getProperty(DeleteRetentionMsProp, Defaults.DeleteRetentionMs.toString())),
                new Double(props.getProperty(MinCleanableDirtyRatioProp, Defaults.MinCleanableDirtyRatio.toString())),
                new Boolean(!props.getProperty(CleanupPolicyProp, Defaults.Compact ? "compact" : "delete").trim().toLowerCase().equals("delete")),
                new Boolean(props.getProperty(UncleanLeaderElectionEnableProp,
                        Defaults.UncleanLeaderElectionEnable.toString())),
                new Integer(props.getProperty(MinInSyncReplicasProp, Defaults.MinInSyncReplicas.toString())));
    }


    public Properties toProps() {
        Properties props = new Properties();
        props.put(SegmentBytesProp, segmentSize.toString());
        props.put(SegmentMsProp, segmentMs.toString());
        props.put(SegmentJitterMsProp, segmentJitterMs.toString());
        props.put(SegmentIndexBytesProp, maxIndexSize.toString());
        props.put(FlushMessagesProp, flushInterval.toString());
        props.put(FlushMsProp, flushMs.toString());
        props.put(RetentionBytesProp, retentionSize.toString());
        props.put(RententionMsProp, retentionMs.toString());
        props.put(MaxMessageBytesProp, maxMessageSize.toString());
        props.put(IndexIntervalBytesProp, indexInterval.toString());
        props.put(DeleteRetentionMsProp, deleteRetentionMs.toString());
        props.put(FileDeleteDelayMsProp, fileDeleteDelayMs.toString());
        props.put(MinCleanableDirtyRatioProp, minCleanableRatio.toString());
        props.put(CleanupPolicyProp, compact ? "compact" : "delete");
        props.put(UncleanLeaderElectionEnableProp, uncleanLeaderElectionEnable.toString());
        props.put(MinInSyncReplicasProp, minInSyncReplicas.toString());
        return props;
    }

    public Long randomSegmentJitter;

    public void init() {
        if (segmentJitterMs == 0) {
            randomSegmentJitter = 0L;
        } else {
            randomSegmentJitter = Math.abs(new Random().nextInt()) % Math.min(segmentJitterMs, segmentMs);
        }
    }

    public static final String SegmentBytesProp = "segment.bytes";
    public static final String SegmentMsProp = "segment.ms";
    public static final String SegmentJitterMsProp = "segment.jitter.ms";
    public static final String SegmentIndexBytesProp = "segment.index.bytes";
    public static final String FlushMessagesProp = "flush.messages";
    public static final String FlushMsProp = "flush.ms";
    public static final String RetentionBytesProp = "retention.bytes";
    public static final String RententionMsProp = "retention.ms";
    public static final String MaxMessageBytesProp = "max.message.bytes";
    public static final String IndexIntervalBytesProp = "index.interval.bytes";
    public static final String DeleteRetentionMsProp = "delete.retention.ms";
    public static final String FileDeleteDelayMsProp = "file.delete.delay.ms";
    public static final String MinCleanableDirtyRatioProp = "min.cleanable.dirty.ratio";
    public static final String CleanupPolicyProp = "cleanup.policy";
    public static final String UncleanLeaderElectionEnableProp = "unclean.leader.election.enable";
    public static final String MinInSyncReplicasProp = "min.insync.replicas";

    public static final Set<String> ConfigNames = Sets.newHashSet(SegmentBytesProp,
            SegmentMsProp,
            SegmentJitterMsProp,
            SegmentIndexBytesProp,
            FlushMessagesProp,
            FlushMsProp,
            RetentionBytesProp,
            RententionMsProp,
            MaxMessageBytesProp,
            IndexIntervalBytesProp,
            FileDeleteDelayMsProp,
            DeleteRetentionMsProp,
            MinCleanableDirtyRatioProp,
            CleanupPolicyProp,
            UncleanLeaderElectionEnableProp,
            MinInSyncReplicasProp);


    /**
     * Create a log config instance using the given properties and defaults
     */
    public static LogConfig fromProps(Properties defaults, Properties overrides) {
        Properties props = new Properties(defaults);
        props.putAll(overrides);
        return fromProps(props);
    }

    /**
     * Check that property names are valid
     */
    public  static void validateNames(Properties props) {
        for (Object name : props.keySet())
            Prediction.require(LogConfig.ConfigNames.contains(name), String.format("Unknown configuration \"%s\".", name));
    }

    /**
     * Check that the given properties contain only valid log config names, and that all values can be parsed.
     */
    public static void validate(Properties props) {
        validateNames(props);
        validateMinInSyncReplicas(props);
        LogConfig.fromProps(new LogConfig().toProps(), props); // check that we can parse the values;
    }

    /**
     * Check that MinInSyncReplicas is reasonable
     * Unfortunately, we can't validate its smaller than number of replicas
     * since we don't have this information here
     */
    private static  void validateMinInSyncReplicas(Properties props) {
        Object minIsr = props.getProperty(MinInSyncReplicasProp);
        if (minIsr != null && new Integer(minIsr.toString()) < 1) {
            throw new InvalidConfigException("Wrong value " + minIsr + " of min.insync.replicas in topic configuration; " +
                    " Valid values are at least 1");
        }
    }

    @Override
    protected LogConfig clone() throws CloneNotSupportedException {
        return (LogConfig)super.clone();
    }

    public LogConfig copy(Integer segmentSize){
        LogConfig log = null;
        try {
            log = clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        log.segmentSize = segmentSize;
        return log;
    }
}
