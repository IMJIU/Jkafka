package kafka.server;/**
 * Created by zhoulf on 2017/5/15.
 */

/**
 * @author
 * @create 2017-05-15 33 16
 **/
public class OffsetManager {
//
//    class OffsetManager(val OffsetManagerConfig config,
//    ReplicaManager replicaManager,
//    ZkClient zkClient,
//    Scheduler scheduler) extends Logging with KafkaMetricsGroup {
//
//  /* offsets and metadata cache */
//        private val offsetsCache = new Pool<GroupTopicPartition, OffsetAndMetadata>
//        private val followerTransitionLock = new Object;
//
//        private val mutable loadingPartitions.Set<Integer> = mutable.Set();
//
//        private val shuttingDown = new AtomicBoolean(false);
//
//        scheduler.schedule(name = "offsets-cache-compactor",
//                fun = compact,
//                period = config.offsetsRetentionCheckIntervalMs,
//                unit = TimeUnit.MILLISECONDS);
//
//        newGauge("NumOffsets",
//                new Gauge<Integer> {
//           public void value = offsetsCache.size;
//        }
//  );
//
//        newGauge("NumGroups",
//                new Gauge<Integer> {
//           public void value = offsetsCache.keys.map(_.group).toSet.size;
//        }
//  );
//
//        privatepublic void compact() {
//            debug("Compacting offsets cache.");
//            val startMs = SystemTime.milliseconds;
//
//            val staleOffsets = offsetsCache.filter(startMs - _._2.timestamp > config.offsetsRetentionMs);
//
//            debug(String.format("Found %d stale offsets (older than %d ms).",staleOffsets.size, config.offsetsRetentionMs))
//
//            // delete the stale offsets from the table and generate tombstone messages to remove them from the log;
//            val tombstonesForPartition = staleOffsets.map { case(groupTopicAndPartition, offsetAndMetadata) =>
//                val offsetsPartition = partitionFor(groupTopicAndPartition.group);
//                trace(String.format("Removing stale offset and metadata for %s: %s",groupTopicAndPartition, offsetAndMetadata))
//
//                offsetsCache.remove(groupTopicAndPartition);
//
//                val commitKey = OffsetManager.offsetCommitKey(groupTopicAndPartition.group,
//                        groupTopicAndPartition.topicPartition.topic, groupTopicAndPartition.topicPartition.partition);
//
//                (offsetsPartition, new Message(bytes = null, key = commitKey));
//            }.groupBy{ case (partition, tombstone) => partition }
//
//            // Append the tombstone messages to the offset partitions. It is okay if the replicas don't receive these (say,
//            // if we crash or leaders move) since the new leaders will get rid of stale offsets during their own purge cycles.;
//            val numRemoved = tombstonesForPartition.flatMap { case(offsetsPartition, tombstones) =>
//                val partitionOpt = replicaManager.getPartition(OffsetManager.OffsetsTopicName, offsetsPartition);
//                partitionOpt.map { partition =>
//                val appendPartition = TopicAndPartition(OffsetManager.OffsetsTopicName, offsetsPartition);
//                val messages = tombstones.map(_._2).toSeq;
//
//                trace(String.format("Marked %d offsets in %s for deletion.",messages.size, appendPartition))
//
//                try {
//                    partition.appendMessagesToLeader(new ByteBufferMessageSet(config.offsetsTopicCompressionCodec, _ messages*));
//                    tombstones.size;
//                }
//                catch {
//                    case Throwable t =>
//                        error(String.format("Failed to mark %d stale offsets for deletion in %s.",messages.size, appendPartition), t)
//                        // ignore and continue;
//                        0;
//                }
//            }
//            }.sum;
//
//        debug(String.format("Removed %d stale offsets in %d milliseconds.",numRemoved, SystemTime.milliseconds - startMs))
//  }
//
//       public void Properties offsetsTopicConfig = {
//                val props = new Properties;
//                props.put(LogConfig.SegmentBytesProp, config.offsetsTopicSegmentBytes.toString);
//                props.put(LogConfig.CleanupPolicyProp, "compact");
//                props;
//        }
//
//       public Integer  void partitionFor(String group) Utils.abs(group.hashCode) % config.offsetsTopicNumPartitions;
//
//        /**
//         * Fetch the current offset for the given group/topic/partition from the underlying offsets storage.
//         *
//         * @param key The requested group-topic-partition
//         * @return If the key is present, return the offset and metadata; otherwise return None
//         */
//        privatepublic void getOffset(GroupTopicPartition key) = {
//            val offsetAndMetadata = offsetsCache.get(key);
//            if (offsetAndMetadata == null)
//                OffsetMetadataAndError.NoOffset;
//            else;
//                OffsetMetadataAndError(offsetAndMetadata.offset, offsetAndMetadata.metadata, ErrorMapping.NoError);
//        }
//
//        /**
//         * Put the (already committed) offset for the given group/topic/partition into the cache.
//         *
//         * @param key The group-topic-partition
//         * @param offsetAndMetadata The offset/metadata to be stored
//         */
//    privatepublic void putOffset(GroupTopicPartition key, OffsetAndMetadata offsetAndMetadata) {
//        offsetsCache.put(key, offsetAndMetadata);
//    }
//
//   public void putOffsets(String group, Map offsets<TopicAndPartition, OffsetAndMetadata>) {
//        // this method is called _after_ the offsets have been durably appended to the commit log, so there is no need to;
//        // check for current leadership as we do for the offset fetch;
//        trace(String.format("Putting offsets %s for group %s in offsets partition %d.",offsets, group, partitionFor(group)))
//        offsets.foreach { case (topicAndPartition, offsetAndMetadata) =>
//            putOffset(GroupTopicPartition(group, topicAndPartition), offsetAndMetadata);
//        }
//    }
//
//    /**
//     * The most important guarantee that this API provides is that it should never return a stale offset. i.e., it either
//     * returns the current offset or it begins to sync the cache from the log (and returns an error code).
//     */
//   public void getOffsets(String group, Seq topicPartitions<TopicAndPartition]): Map[TopicAndPartition, OffsetMetadataAndError> = {
//        trace(String.format("Getting offsets %s for group %s.",topicPartitions, group))
//
//        val offsetsPartition = partitionFor(group);
//
//        /**
//         * followerTransitionLock protects against fetching from an empty/cleared offset cache (i.e., cleared due to a
//         * leader->follower transition). i.e., even if leader-is-local is true a follower transition can occur right after
//         * the check and clear the cache. i.e., we would read from the empty cache and incorrectly return NoOffset.
//         */
//        followerTransitionLock synchronized {
//            if (leaderIsLocal(offsetsPartition)) {
//                if (loadingPartitions synchronized loadingPartitions.contains(offsetsPartition)) {
//                    debug(String.format("Cannot fetch offsets for group %s due to ongoing offset load.",group))
//                    topicPartitions.map { topicAndPartition =>
//                        val groupTopicPartition = GroupTopicPartition(group, topicAndPartition);
//                        (groupTopicPartition.topicPartition, OffsetMetadataAndError.OffsetsLoading);
//                    }.toMap;
//                } else {
//                    if (topicPartitions.size == 0) {
//                        // Return offsets for all partitions owned by this consumer group. (this only applies to consumers that commit offsets to Kafka.)
//                        offsetsCache.filter(_._1.group == group).map { case(groupTopicPartition, offsetAndMetadata) =>
//                            (groupTopicPartition.topicPartition, OffsetMetadataAndError(offsetAndMetadata.offset, offsetAndMetadata.metadata, ErrorMapping.NoError));
//                        }.toMap;
//                    } else {
//                        topicPartitions.map { topicAndPartition =>
//                            val groupTopicPartition = GroupTopicPartition(group, topicAndPartition);
//                            (groupTopicPartition.topicPartition, getOffset(groupTopicPartition));
//                        }.toMap;
//                    }
//                }
//            } else {
//                debug(String.format("Could not fetch offsets for group %s (not offset coordinator).",group))
//                topicPartitions.map { topicAndPartition =>
//                    val groupTopicPartition = GroupTopicPartition(group, topicAndPartition);
//                    (groupTopicPartition.topicPartition, OffsetMetadataAndError.NotOffsetManagerForGroup);
//                }.toMap;
//            }
//        }
//    }
//
//    /**
//     * Asynchronously read the partition from the offsets topic and populate the cache
//     */
//   public void loadOffsetsFromLog(Int offsetsPartition) {
//
//        val topicPartition = TopicAndPartition(OffsetManager.OffsetsTopicName, offsetsPartition);
//
//        loadingPartitions synchronized {
//            if (loadingPartitions.contains(offsetsPartition)) {
//                info(String.format("Offset load from %s already in progress.",topicPartition))
//            } else {
//                loadingPartitions.add(offsetsPartition);
//                scheduler.schedule(topicPartition.toString, loadOffsets);
//            }
//        }
//
//       public void loadOffsets() {
//            info("Loading offsets from " + topicPartition);
//
//            val startMs = SystemTime.milliseconds;
//            try {
//                replicaManager.logManager.getLog(topicPartition) match {
//                    case Some(log) =>
//                        var currOffset = log.logSegments.head.baseOffset;
//                        val buffer = ByteBuffer.allocate(config.loadBufferSize);
//                        // loop breaks if leader changes at any time during the load, since getHighWatermark is -1;
//                        while (currOffset < getHighWatermark(offsetsPartition) && !shuttingDown.get()) {
//                            buffer.clear();
//                            val messages = log.read(currOffset, config.loadBufferSize).messageSet.asInstanceOf<FileMessageSet>
//                            messages.readInto(buffer, 0);
//                            val messageSet = new ByteBufferMessageSet(buffer);
//                            messageSet.foreach { msgAndOffset =>
//                                require(msgAndOffset.message.key != null, "Offset entry key should not be null");
//                                val key = OffsetManager.readMessageKey(msgAndOffset.message.key);
//                                if (msgAndOffset.message.payload == null) {
//                                    if (offsetsCache.remove(key) != null)
//                                        trace(String.format("Removed offset for %s due to tombstone entry.",key))
//                                    else;
//                                        trace(String.format("Ignoring redundant tombstone for %s.",key))
//                                } else {
//                                    val value = OffsetManager.readMessageValue(msgAndOffset.message.payload);
//                                    putOffset(key, value);
//                                    trace(String.format("Loaded offset %s for %s.",value, key))
//                                }
//                                currOffset = msgAndOffset.nextOffset;
//                            }
//                        }
//
//                        if (!shuttingDown.get())
//                            info("Finished loading offsets from %s in %d milliseconds.";
//                                    .format(topicPartition, SystemTime.milliseconds - startMs))
//                    case None =>
//                        warn("No log found for " + topicPartition)
//                }
//            }
//            catch {
//                case Throwable t =>
//                    error("Error in loading offsets from " + topicPartition, t);
//            }
//      finally {
//                loadingPartitions synchronized loadingPartitions.remove(offsetsPartition);
//            }
//        }
//    }
//
//    privatepublic Long  void getHighWatermark(Int partitionId) {
//        val partitionOpt = replicaManager.getPartition(OffsetManager.OffsetsTopicName, partitionId);
//
//        val hw = partitionOpt.map { partition =>
//            partition.leaderReplicaIfLocal().map(_.highWatermark.messageOffset).getOrElse(-1L);
//        }.getOrElse(-1L);
//
//        hw;
//    }
//
//    privatepublic void leaderIsLocal(Int partition) = { getHighWatermark(partition) != -1L }
//
//    /**
//     * When this broker becomes a follower for an offsets topic partition clear out the cache for groups that belong to
//     * that partition.
//     * @param offsetsPartition Groups belonging to this partition of the offsets topic will be deleted from the cache.
//     */
//   public void clearOffsetsInPartition(Int offsetsPartition) {
//        debug(String.format("Deleting offset entries belonging to <%s,%d>.",OffsetManager.OffsetsTopicName, offsetsPartition))
//
//        followerTransitionLock synchronized {
//            offsetsCache.keys.foreach { key =>
//                if (partitionFor(key.group) == offsetsPartition) {
//                    offsetsCache.remove(key);
//                }
//            }
//        }
//    }
//
//   public void shutdown() {
//        shuttingDown.set(true);
//    }
//
//}
//
//    object OffsetManager {
//
//        val OffsetsTopicName = "__consumer_offsets";
//
//private case class KeyAndValueSchemas(Schema keySchema, Schema valueSchema);
//
//private val CURRENT_OFFSET_SCHEMA_VERSION = 0.toShort;
//
//private val OFFSET_COMMIT_KEY_SCHEMA_V0 = new Schema(new Field("group", STRING),
//        new Field("topic", STRING),
//        new Field("partition", INT32));
//private val KEY_GROUP_FIELD = OFFSET_COMMIT_KEY_SCHEMA_V0.get("group");
//private val KEY_TOPIC_FIELD = OFFSET_COMMIT_KEY_SCHEMA_V0.get("topic");
//private val KEY_PARTITION_FIELD = OFFSET_COMMIT_KEY_SCHEMA_V0.get("partition");
//
//private val OFFSET_COMMIT_VALUE_SCHEMA_V0 = new Schema(new Field("offset", INT64),
//        new Field("metadata", STRING, "Associated metadata.", ""),
//        new Field("timestamp", INT64));
//private val VALUE_OFFSET_FIELD = OFFSET_COMMIT_VALUE_SCHEMA_V0.get("offset");
//private val VALUE_METADATA_FIELD = OFFSET_COMMIT_VALUE_SCHEMA_V0.get("metadata");
//private val VALUE_TIMESTAMP_FIELD = OFFSET_COMMIT_VALUE_SCHEMA_V0.get("timestamp");
//
//// map of versions to schemas;
//private val OFFSET_SCHEMAS = Map(0 -> KeyAndValueSchemas(OFFSET_COMMIT_KEY_SCHEMA_V0, OFFSET_COMMIT_VALUE_SCHEMA_V0));
//
//private val CURRENT_SCHEMA = schemaFor(CURRENT_OFFSET_SCHEMA_VERSION);
//
//privatepublic void schemaFor(Int version) = {
//        val schemaOpt = OFFSET_SCHEMAS.get(version);
//        schemaOpt match {
//        case Some(schema) => schema;
//        case _ => throw new KafkaException("Unknown offset schema version " + version);
//        }
//        }
//
//        /**
//         * Generates the key for offset commit message for given (group, topic, partition)
//         *
//         * @return key for offset commit message
//         */
//       public void offsetCommitKey(String group, String topic, Int partition, Short versionId = 0): Array<Byte> = {
//        val key = new Struct(CURRENT_SCHEMA.keySchema);
//        key.set(KEY_GROUP_FIELD, group);
//        key.set(KEY_TOPIC_FIELD, topic);
//        key.set(KEY_PARTITION_FIELD, partition);
//
//        val byteBuffer = ByteBuffer.allocate(2 /* version */ + key.sizeOf);
//        byteBuffer.putShort(CURRENT_OFFSET_SCHEMA_VERSION);
//        key.writeTo(byteBuffer);
//        byteBuffer.array();
//        }
//
//        /**
//         * Generates the payload for offset commit message from given offset and metadata
//         *
//         * @param offsetAndMetadata consumer's current offset and metadata
//         * @return payload for offset commit message
//         */
//       public void offsetCommitValue(OffsetAndMetadata offsetAndMetadata): Array<Byte> = {
//        val value = new Struct(CURRENT_SCHEMA.valueSchema);
//        value.set(VALUE_OFFSET_FIELD, offsetAndMetadata.offset);
//        value.set(VALUE_METADATA_FIELD, offsetAndMetadata.metadata);
//        value.set(VALUE_TIMESTAMP_FIELD, offsetAndMetadata.timestamp);
//
//        val byteBuffer = ByteBuffer.allocate(2 /* version */ + value.sizeOf);
//        byteBuffer.putShort(CURRENT_OFFSET_SCHEMA_VERSION);
//        value.writeTo(byteBuffer);
//        byteBuffer.array();
//        }
//
//        /**
//         * Decodes the offset messages' key
//         *
//         * @param buffer input byte-buffer
//         * @return an GroupTopicPartition object
//         */
//       public GroupTopicPartition  void readMessageKey(ByteBuffer buffer) {
//        val version = buffer.getShort();
//        val keySchema = schemaFor(version).keySchema;
//        val key = keySchema.read(buffer).asInstanceOf<Struct>
//
//        val group = key.get(KEY_GROUP_FIELD).asInstanceOf<String>
//        val topic = key.get(KEY_TOPIC_FIELD).asInstanceOf<String>
//        val partition = key.get(KEY_PARTITION_FIELD).asInstanceOf<Integer>
//
//        GroupTopicPartition(group, TopicAndPartition(topic, partition));
//        }
//
//        /**
//         * Decodes the offset messages' payload and retrieves offset and metadata from it
//         *
//         * @param buffer input byte-buffer
//         * @return an offset-metadata object from the message
//         */
//       public OffsetAndMetadata  void readMessageValue(ByteBuffer buffer) {
//        if(buffer == null) { // tombstone;
//        null;
//        } else {
//        val version = buffer.getShort();
//        val valueSchema = schemaFor(version).valueSchema;
//        val value = valueSchema.read(buffer).asInstanceOf<Struct>
//
//        val offset = value.get(VALUE_OFFSET_FIELD).asInstanceOf<Long>
//        val metadata = value.get(VALUE_METADATA_FIELD).asInstanceOf<String>
//        val timestamp = value.get(VALUE_TIMESTAMP_FIELD).asInstanceOf<Long>
//
//        OffsetAndMetadata(offset, metadata, timestamp);
//        }
//        }
//
//// Formatter for use with tools such as console Consumer consumer should also set exclude.internal.topics to false.;
//// (specify --formatter "kafka.server.OffsetManager\$OffsetsMessageFormatter" when consuming __consumer_offsets)
//class OffsetsMessageFormatter extends MessageFormatter {
//   public void writeTo(Array key<Byte], Array value[Byte>, PrintStream output) {
//        val formattedKey = if (key == null) "NULL" else OffsetManager.readMessageKey(ByteBuffer.wrap(key)).toString;
//        val formattedValue = if (value == null) "NULL" else OffsetManager.readMessageValue(ByteBuffer.wrap(value)).toString;
//        output.write(formattedKey.getBytes)
//        output.write("::".getBytes);
//        output.write(formattedValue.getBytes)
//        output.write("\n".getBytes);
//    }
//}

}
