package kafka.server;/**
 * Created by zhoulf on 2017/5/15.
 */

import com.google.common.collect.ImmutableMap;
import com.yammer.metrics.core.Gauge;
import kafka.cluster.Partition;
import kafka.common.ErrorMapping;
import kafka.common.KafkaException;
import kafka.common.OffsetAndMetadata;
import kafka.common.OffsetMetadataAndError;
import kafka.func.Action;
import kafka.func.IntCount;
import kafka.func.Tuple;
import kafka.log.FileMessageSet;
import kafka.log.Log;
import kafka.log.LogConfig;
import kafka.log.TopicAndPartition;
import kafka.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import kafka.metrics.KafkaMetricsGroup;
import kafka.utils.*;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * @author
 * @create 2017-05-15 33 16
 **/
public class OffsetManager extends KafkaMetricsGroup {
    //
   public OffsetManagerConfig config;
    public ReplicaManager replicaManager;
    public ZkClient zkClient;
    public  Scheduler scheduler;

    public OffsetManager(OffsetManagerConfig config, ReplicaManager replicaManager, ZkClient zkClient, Scheduler scheduler) {
        this.config = config;
        this.replicaManager = replicaManager;
        this.zkClient = zkClient;
        this.scheduler = scheduler;
    }

    /* offsets and metadata cache */
    private Pool<GroupTopicPartition, OffsetAndMetadata> offsetsCache = new Pool<>();
    private Object followerTransitionLock = new Object();

    private Set<Integer> loadingPartitions = new HashSet();

    private AtomicBoolean shuttingDown = new AtomicBoolean(false);

    public void init() {
        scheduler.schedule("offsets-cache-compactor",
                compact,
                0L,
                config.DefaultOffsetsRetentionCheckIntervalMs,
                TimeUnit.MILLISECONDS);

        newGauge("NumOffsets", new Gauge<Object>() {
            public Object value() {
                return offsetsCache.getSize();
            }
        });
        newGauge("NumGroups", new Gauge<Integer>() {
            public Integer value() {
                return offsetsCache.keys().stream().map(t -> t.group).collect(Collectors.toSet()).size();
            }
        });
    }

    //
    private Action compact = () -> {
        debug("Compacting offsets cache.");
        long startMs = Time.get().milliseconds();

        List<Tuple<GroupTopicPartition, OffsetAndMetadata>> staleOffsets = Itor.filter(offsetsCache.iterator(), c -> (startMs - c.v2.timestamp > config.offsetsRetentionMs));

        debug(String.format("Found %d stale offsets (older than %d ms).", staleOffsets.size(), config.offsetsRetentionMs));


        // delete the stale offsets from the table and generate tombstone messages to remove them from the log;
        List<Tuple<Integer, Message>> list = Utils.map(staleOffsets,t -> {
            GroupTopicPartition groupTopicAndPartition = t.v1;
            OffsetAndMetadata offsetAndMetadata = t.v2;
            Integer offsetsPartition = partitionFor(groupTopicAndPartition.group);
            trace(String.format("Removing stale offset and metadata for %s: %s", groupTopicAndPartition, offsetAndMetadata));
            offsetsCache.remove(groupTopicAndPartition);
            byte[] commitKey = OffsetManager.offsetCommitKey(groupTopicAndPartition.group,
                    groupTopicAndPartition.topicPartition.topic, groupTopicAndPartition.topicPartition.partition);
            return Tuple.of(offsetsPartition, new Message(null, commitKey));
        });

        Map<Integer, List<Tuple<Integer, Message>>> tombstonesForPartition = Utils.groupBy(list, t -> t.v1);
        // Append the tombstone messages to the offset partitions. It is okay if the replicas don't receive these (say,
        // if we crash or leaders move) since the new leaders will get rid of stale offsets during their own purge cycles.;
        IntCount numRemoved = IntCount.of(0);
                Utils.foreach(tombstonesForPartition,(offsetsPartition,tombstones)->{
                    Optional<Partition> partitionOpt = replicaManager.getPartition(OffsetManager.OffsetsTopicName, offsetsPartition);
                    if(partitionOpt.isPresent()){
                        Partition partition = partitionOpt.get();
                        TopicAndPartition appendPartition = new TopicAndPartition(OffsetManager.OffsetsTopicName, offsetsPartition);
                        List<Message> messages = Utils.map(tombstones,t->t.v2);
                        trace(String.format("Marked %d offsets in %s for deletion.", messages.size()  , appendPartition));
                        try {
                            partition.appendMessagesToLeader(new ByteBufferMessageSet(config.offsetsTopicCompressionCodec, messages));
                            numRemoved.add(tombstones.size());
                        } catch ( Throwable t){
                            error(String.format("Failed to mark %d stale offsets for deletion in %s.", messages.size(), appendPartition), t);
                            // ignore and continue;
                        }
                    }
                });
//                tombstonesForPartition.flatMap {}.sum();

        debug(String.format("Removed %d stale offsets in %d milliseconds.", numRemoved.get(), Time.get().milliseconds() - startMs));
    };

    public Properties offsetsTopicConfig() {
        Properties props = new Properties();
        props.put(LogConfig.SegmentBytesProp, config.offsetsTopicSegmentBytes.toString());
        props.put(LogConfig.CleanupPolicyProp, "compact");
        return props;
    }

    public Integer partitionFor(String group) {
        return Utils.abs(group.hashCode()) % config.offsetsTopicNumPartitions;
    }

    /**
     * Fetch the current offset for the given group/topic/partition from the underlying offsets storage.
     *
     * @param key The requested group-topic-partition
     * @return If the key is present, return the offset and metadata; otherwise return None
     */
    private OffsetMetadataAndError getOffset(GroupTopicPartition key) {
        OffsetAndMetadata offsetAndMetadata = offsetsCache.get(key);
        if (offsetAndMetadata == null)
            return OffsetMetadataAndError.NoOffset;
        else
            return new OffsetMetadataAndError(offsetAndMetadata.offset, offsetAndMetadata.metadata, ErrorMapping.NoError);
    }

    /**
     * Put the (already committed) offset for the given group/topic/partition into the cache.
     *
     * @param key               The group-topic-partition
     * @param offsetAndMetadata The offset/metadata to be stored
     */
    private void putOffset(GroupTopicPartition key, OffsetAndMetadata offsetAndMetadata) {
        offsetsCache.put(key, offsetAndMetadata);
    }

    public void putOffsets(String group, Map<TopicAndPartition, OffsetAndMetadata> offsets) {
        // this method is called _after_ the offsets have been durably appended to the commit log, so there is no need to;
        // check for current leadership as we do for the offset fetch;
        trace(String.format("Putting offsets %s for group %s in offsets partition %d.", offsets, group, partitionFor(group)));
        offsets.forEach((topicAndPartition, offsetAndMetadata) -> putOffset(new GroupTopicPartition(group, topicAndPartition), offsetAndMetadata));
    }

    /**
     * The most important guarantee that this API provides is that it should never return a stale offset. i.e., it either
     * returns the current offset or it begins to sync the cache from the log (and returns an error code).
     */
    public Map<TopicAndPartition, OffsetMetadataAndError> getOffsets(String group, List<TopicAndPartition> topicPartitions) {
        trace(String.format("Getting offsets %s for group %s.", topicPartitions, group));

        Integer offsetsPartition = partitionFor(group);

        /**
         * followerTransitionLock protects against fetching from an empty/cleared offset cache (i.e., cleared due to a
         * leader->follower transition). i.e., even if leader-is-local is true a follower transition can occur right after
         * the check and clear the cache. i.e., we would read from the empty cache and incorrectly return NoOffset.
         */
        synchronized (followerTransitionLock) {
            if (leaderIsLocal(offsetsPartition)) {
                boolean contains;
                synchronized (loadingPartitions) {
                    contains = loadingPartitions.contains(offsetsPartition);
                }
                if (contains) {
                    debug(String.format("Cannot fetch offsets for group %s due to ongoing offset load.", group));
                    return Utils.toMap(Utils.map(topicPartitions, topicAndPartition -> {
                        GroupTopicPartition groupTopicPartition = new GroupTopicPartition(group, topicAndPartition);
                        return Tuple.of(groupTopicPartition.topicPartition, OffsetMetadataAndError.OffsetsLoading);
                    }));
                } else {
                    if (topicPartitions.size() == 0) {
                        // Return offsets for all partitions owned by this consumer group. (this only applies to consumers that commit offsets to Kafka.)
                        return Utils.toMap(Utils.map(Utils.filter(offsetsCache, c -> c.v1.group == group), kv -> {
                            GroupTopicPartition groupTopicPartition = kv.v1;
                            OffsetAndMetadata offsetAndMetadata = kv.v2;
                            return Tuple.of(groupTopicPartition.topicPartition, new OffsetMetadataAndError(offsetAndMetadata.offset, offsetAndMetadata.metadata, ErrorMapping.NoError));
                        }));
                    } else {
                        return Utils.toMap(Utils.map(topicPartitions, topicAndPartition -> {
                            GroupTopicPartition groupTopicPartition = new GroupTopicPartition(group, topicAndPartition);
                            return Tuple.of(groupTopicPartition.topicPartition, getOffset(groupTopicPartition));
                        }));
                    }
                }
            } else {
                debug(String.format("Could not fetch offsets for group %s (not offset coordinator).", group));
                return Utils.toMap(Utils.map(topicPartitions, topicAndPartition -> {
                    GroupTopicPartition groupTopicPartition = new GroupTopicPartition(group, topicAndPartition);
                    return Tuple.of(groupTopicPartition.topicPartition, OffsetMetadataAndError.NotOffsetManagerForGroup);
                }));
            }
        }
    }

    /**
     * Asynchronously read the partition from the offsets topic and populate the cache
     */
    public void loadOffsetsFromLog(Integer offsetsPartition) {

        TopicAndPartition topicPartition = new TopicAndPartition(OffsetManager.OffsetsTopicName, offsetsPartition);

        synchronized (loadingPartitions) {
            if (loadingPartitions.contains(offsetsPartition)) {
                info(String.format("Offset load from %s already in progress.", topicPartition));
            } else {
                loadingPartitions.add(offsetsPartition);
                scheduler.schedule(topicPartition.toString(), ()->{
                    info("Loading offsets from " + topicPartition);
                    Long startMs = Time.get().milliseconds();
                    try {
                        Optional<Log> logOpt = replicaManager.logManager.getLog(topicPartition);
                        if (logOpt.isPresent()) {
                            Log log = logOpt.get();
                            long currOffset = log.logSegments().iterator().next().baseOffset;
                            ByteBuffer buffer = ByteBuffer.allocate(config.loadBufferSize);
                            // loop breaks if leader changes at any time during the load, since getHighWatermark is -1;
                            while (currOffset < getHighWatermark(offsetsPartition) && !shuttingDown.get()) {
                                buffer.clear();
                                FileMessageSet messages = (FileMessageSet) log.read(currOffset, config.loadBufferSize).messageSet;
                                messages.readInto(buffer, 0);
                                ByteBufferMessageSet messageSet = new ByteBufferMessageSet(buffer);
                                for (MessageAndOffset msgAndOffset : messageSet) {
                                    Prediction.require(msgAndOffset.message.key() != null, "Offset entry key should not be null");
                                    GroupTopicPartition key = OffsetManager.readMessageKey(msgAndOffset.message.key());
                                    if (msgAndOffset.message.payload() == null) {
                                        if (offsetsCache.remove(key) != null)
                                            trace(String.format("Removed offset for %s due to tombstone entry.", key));
                                        else ;
                                        trace(String.format("Ignoring redundant tombstone for %s.", key));
                                    } else {
                                        OffsetAndMetadata value = OffsetManager.readMessageValue(msgAndOffset.message.payload());
                                        putOffset(key, value);
                                        trace(String.format("Loaded offset %s for %s.", value, key));
                                    }
                                    currOffset = msgAndOffset.nextOffset();
                                }

                                if (!shuttingDown.get())
                                    info(String.format("Finished loading offsets from %s in %d milliseconds.", topicPartition, Time.get().milliseconds() - startMs));
                            }
                        } else {
                            warn("No log found for " + topicPartition);
                        }
                    } catch (Throwable t) {
                        error("Error in loading offsets from " + topicPartition, t);
                    } finally {
                        synchronized (loadingPartitions) {
                            loadingPartitions.remove(offsetsPartition);
                        }
                    }
                },0L);
            }
        }
    }


    private Long getHighWatermark(Integer partitionId) {
        Optional<Partition> partitionOpt = replicaManager.getPartition(OffsetManager.OffsetsTopicName, partitionId);
        Long hw;
        if (partitionOpt.isPresent()) {
            Optional<Long> opt = partitionOpt.get().leaderReplicaIfLocal().map(l -> l.highWatermark.messageOffset);
            if (opt.isPresent()) {
                hw = opt.get();
            } else {
                hw = -1L;
            }
        } else {
            hw = -1L;
        }
        return hw;
    }

    private boolean leaderIsLocal(Integer partition) {
        return getHighWatermark(partition) != -1L;
    }

    /**
     * When this broker becomes a follower for an offsets topic partition clear out the cache for groups that belong to
     * that partition.
     *
     * @param offsetsPartition Groups belonging to this partition of the offsets topic will be deleted from the cache.
     */
    public void clearOffsetsInPartition(Integer offsetsPartition) {
        debug(String.format("Deleting offset entries belonging to <%s,%d>.", OffsetManager.OffsetsTopicName, offsetsPartition));

        synchronized (followerTransitionLock) {
            offsetsCache.keys().forEach(key -> {
                if (partitionFor(key.group) == offsetsPartition) {
                    offsetsCache.remove(key);
                }
            });
        }
    }

    public void shutdown() {
        shuttingDown.set(true);
    }

//--------------------------------------------------------------------------------------
    public static final String OffsetsTopicName = "__consumer_offsets";


    private static class KeyAndValueSchemas {
        public Schema keySchema;
        public Schema valueSchema;

        public KeyAndValueSchemas(Schema keySchema, Schema valueSchema) {
            this.keySchema = keySchema;
            this.valueSchema = valueSchema;
        }
    }

        private static Short CURRENT_OFFSET_SCHEMA_VERSION = 0;

        private static Schema OFFSET_COMMIT_KEY_SCHEMA_V0 = new Schema(new Field("group", Type.STRING),
                new Field("topic", Type.STRING),
                new Field("partition", Type.INT32));
        private static Field KEY_GROUP_FIELD = OFFSET_COMMIT_KEY_SCHEMA_V0.get("group");
        private static Field KEY_TOPIC_FIELD = OFFSET_COMMIT_KEY_SCHEMA_V0.get("topic");
        private static Field KEY_PARTITION_FIELD = OFFSET_COMMIT_KEY_SCHEMA_V0.get("partition");

        private static Schema OFFSET_COMMIT_VALUE_SCHEMA_V0 = new Schema(new Field("offset", Type.INT64),
                new Field("metadata", Type.STRING, "Associated metadata.", ""),
                new Field("timestamp", Type.INT64));
        private static Field VALUE_OFFSET_FIELD = OFFSET_COMMIT_VALUE_SCHEMA_V0.get("offset");
        private static Field VALUE_METADATA_FIELD = OFFSET_COMMIT_VALUE_SCHEMA_V0.get("metadata");
        private static Field VALUE_TIMESTAMP_FIELD = OFFSET_COMMIT_VALUE_SCHEMA_V0.get("timestamp");

        // map of versions to schemas;
        private static Map<Integer, KeyAndValueSchemas> OFFSET_SCHEMAS = ImmutableMap.of(0, new KeyAndValueSchemas(OFFSET_COMMIT_KEY_SCHEMA_V0, OFFSET_COMMIT_VALUE_SCHEMA_V0));

        private static KeyAndValueSchemas CURRENT_SCHEMA = schemaFor(CURRENT_OFFSET_SCHEMA_VERSION.intValue());

        private static KeyAndValueSchemas schemaFor(Integer version) {
            KeyAndValueSchemas schemaOpt = OFFSET_SCHEMAS.get(version.intValue());
            if (schemaOpt == null) {
                throw new KafkaException("Unknown offset schema version " + version);
            }
            return schemaOpt;
        }

        /**
         * Generates the key for offset commit message for given (group, topic, partition)
         *
         * @return key for offset commit message
         */
        public static byte[] offsetCommitKey(String group, String topic, Integer partition) {
            return offsetCommitKey(group, topic, partition, (short) 0);
        }

        public static byte[] offsetCommitKey(String group, String topic, Integer partition, Short versionId) {
            Struct key = new Struct(CURRENT_SCHEMA.keySchema);
            key.set(KEY_GROUP_FIELD, group);
            key.set(KEY_TOPIC_FIELD, topic);
            key.set(KEY_PARTITION_FIELD, partition);

            ByteBuffer byteBuffer = ByteBuffer.allocate(2 /* version */ + key.sizeOf());
            byteBuffer.putShort(CURRENT_OFFSET_SCHEMA_VERSION);
            key.writeTo(byteBuffer);
            return byteBuffer.array();
        }

        /**
         * Generates the payload for offset commit message from given offset and metadata
         *
         * @param offsetAndMetadata consumer's current offset and metadata
         * @return payload for offset commit message
         */
        public static byte[] offsetCommitValue(OffsetAndMetadata offsetAndMetadata) {
            Struct value = new Struct(CURRENT_SCHEMA.valueSchema);
            value.set(VALUE_OFFSET_FIELD, offsetAndMetadata.offset);
            value.set(VALUE_METADATA_FIELD, offsetAndMetadata.metadata);
            value.set(VALUE_TIMESTAMP_FIELD, offsetAndMetadata.timestamp);

            ByteBuffer byteBuffer = ByteBuffer.allocate(2 /* version */ + value.sizeOf());
            byteBuffer.putShort(CURRENT_OFFSET_SCHEMA_VERSION);
            value.writeTo(byteBuffer);
            return byteBuffer.array();
        }

        /**
         * Decodes the offset messages' key
         *
         * @param buffer input byte-buffer
         * @return an GroupTopicPartition object
         */
        public static GroupTopicPartition readMessageKey(ByteBuffer buffer) {
            Short version = buffer.getShort();
            Schema keySchema = schemaFor(version.intValue()).keySchema;
            Struct key = (Struct) keySchema.read(buffer);
            String group = (String) key.get(KEY_GROUP_FIELD);
            String topic = (String) key.get(KEY_TOPIC_FIELD);
            Integer partition = (Integer) key.get(KEY_PARTITION_FIELD);

            return new GroupTopicPartition(group, new TopicAndPartition(topic, partition));
        }

        /**
         * Decodes the offset messages' payload and retrieves offset and metadata from it
         *
         * @param buffer input byte-buffer
         * @return an offset-metadata object from the message
         */
        public static OffsetAndMetadata readMessageValue(ByteBuffer buffer) {
            if (buffer == null) { // tombstone;
                return null;
            } else {
                Short version = buffer.getShort();
                Schema valueSchema = schemaFor(version.intValue()).valueSchema;
                Struct value = (Struct) valueSchema.read(buffer);

                Long offset = (Long) value.get(VALUE_OFFSET_FIELD);
                String metadata = (String) value.get(VALUE_METADATA_FIELD);
                Long timestamp = (Long) value.get(VALUE_TIMESTAMP_FIELD);

                return new OffsetAndMetadata(offset, metadata, timestamp);
            }
        }

        // Formatter for use with tools such as console Consumer consumer should also set exclude.internal.topics to false.;
// (specify --formatter "kafka.server.OffsetManager\$OffsetsMessageFormatter" when consuming __consumer_offsets)
        class OffsetsMessageFormatter extends kafka.tool.MessageFormatter {
            public void writeTo(byte[] key, byte[] value, PrintStream output) throws IOException {
                String formattedKey = (key == null) ? "NULL" : OffsetManager.readMessageKey(ByteBuffer.wrap(key)).toString();
                String formattedValue = (value == null) ? "NULL" : OffsetManager.readMessageValue(ByteBuffer.wrap(value)).toString();
                output.write(formattedKey.getBytes());
                output.write("::".getBytes());
                output.write(formattedValue.getBytes());
                output.write("\n".getBytes());
            }
        }

}