package kafka.server;

/**
 * @author zhoulf
 * @create 2017-10-25 27 13
 **/

import kafka.api.FetchRequest;
import kafka.api.FetchResponse;
import kafka.cluster.Replica;
import kafka.func.IntCount;
import kafka.func.Tuple;
import kafka.log.TopicAndPartition;
import kafka.network.RequestChannel;
import kafka.utils.Utils;
import org.apache.kafka.common.errors.NotLeaderForPartitionException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.util.List;
import java.util.Map;

/**
 * A delayed fetch request, which is satisfied (or more
 * accurately, unblocked) -- if:
 * Case This A broker is no longer the leader for some partitions it tries to fetch
 * - should return whatever data is available for the rest partitions.
 * Case This B broker is does not know of some partitions it tries to fetch
 * - should return whatever data is available for the rest partitions.
 * Case The C fetch offset locates not on the last segment of the log
 * - should return all the data on that segment.
 * Case The D accumulated bytes from all the fetching partitions exceeds the minimum bytes
 * - should return whatever data is available.
 */

public class DelayedFetch extends DelayedRequest {
    // TODO: 2017/10/25 @Overrive属性？？？
//@Override
    public List<TopicAndPartition> keys;
    //@Override
    public RequestChannel.Request request;
    //@Override
    public Long delayMs;
    public FetchRequest fetch;
    private Map<TopicAndPartition, LogOffsetMetadata> partitionFetchOffsets;

    public DelayedFetch(List keys, RequestChannel.Request request, Long delayMs, FetchRequest fetch, Map<TopicAndPartition, LogOffsetMetadata> partitionFetchOffsets) {
        super(keys, request, delayMs);
        this.fetch = fetch;
        this.partitionFetchOffsets = partitionFetchOffsets;
    }

    public DelayedFetch(List<TopicAndPartition> keys, RequestChannel.Request request, Long delayMs) {
        super(keys, request, delayMs);
//        this.keys = keys;
//        this.request = request;
//        this.delayMs = delayMs;
    }



    public Boolean isSatisfied(ReplicaManager replicaManager) {
        IntCount accumulatedSize = IntCount.of(0);
        boolean fromFollower = fetch.isFromFollower();
        IntCount error = IntCount.of(0);
        partitionFetchOffsets.forEach((topicAndPartition, fetchOffset) -> {
            if (error.get() == 0) {
                try {
                    if (fetchOffset != LogOffsetMetadata.UnknownOffsetMetadata) {
                        Replica replica = replicaManager.getLeaderReplicaIfLocal(topicAndPartition.topic, topicAndPartition.partition);
                        LogOffsetMetadata endOffset;
                        if (fromFollower)
                            endOffset = replica.logEndOffset();
                        else
                            endOffset = replica.highWatermark();

                        if (endOffset.offsetOnOlderSegment(fetchOffset)) {
                            // Case C, this can happen when the new follower replica fetching on a truncated leader;
                            debug(String.format("Satisfying fetch request %s since it is fetching later segments of partition %s.", fetch, topicAndPartition));
                            error.set(1);
                        } else if (fetchOffset.offsetOnOlderSegment(endOffset)) {
                            // Case C, this can happen when the folloer replica is lagging too much;
                            debug(String.format("Satisfying fetch request %s immediately since it is fetching older segments.", fetch));
                            error.set(1);
                        } else if (fetchOffset.precedes(endOffset)) {
                            accumulatedSize.add(endOffset.positionDiff(fetchOffset));
                        }
                    }
                } catch (UnknownTopicOrPartitionException utpe) { // Case A;
                    debug(String.format("Broker no longer know of %s, satisfy %s immediately", topicAndPartition, fetch));
                    error.set(1);
                } catch (NotLeaderForPartitionException nle) {  // Case B;
                    debug(String.format("Broker is no longer the leader of %s, satisfy %s immediately", topicAndPartition, fetch));
                    error.set(1);
                }
            }
        });
        if (error.get() == 1) {
            return true;
        }
        // Case D;
        return accumulatedSize.get() >= fetch.minBytes;
    }

    public FetchResponse respond(ReplicaManager replicaManager) {
        Map<TopicAndPartition, PartitionDataAndOffset> topicData = replicaManager.readMessageSets(fetch);
        return new FetchResponse(fetch.correlationId, Utils.mapValue(topicData, v -> v.data));
    }
}
