package kafka.server;


import kafka.api.*;
import kafka.cluster.Partition;
import kafka.common.ErrorMapping;
import kafka.func.Tuple;
import kafka.log.TopicAndPartition;
import kafka.network.RequestChannel;
import kafka.utils.Utils;

import java.util.*;

/**
 * A delayed produce request, which is satisfied (or more
 * accurately, unblocked) -- if for every partition it produce to:
 * Case This A broker is not the unblock leader - should return error.
 * Case This B broker is the leader:
 * B.1 - If there was a localError (when writing to the local log): unblock - should return error
 * B.2 - else, at least requiredAcks replicas should be caught up to this request.
 */

public class DelayedProduce extends DelayedRequest {
    public ProducerRequest produce;
    public Map<TopicAndPartition, DelayedProduceResponseStatus> partitionStatus;
    public Optional<OffsetCommitRequest> offsetCommitRequestOpt = Optional.empty();

    public DelayedProduce(List<TopicAndPartition> keys, RequestChannel.Request request, Long delayMs, ProducerRequest produce, Map<TopicAndPartition, DelayedProduceResponseStatus> partitionStatus, Optional<OffsetCommitRequest> offsetCommitRequestOpt) {
        super(keys, request, delayMs);
        this.produce = produce;
        this.partitionStatus = partitionStatus;
        this.offsetCommitRequestOpt = offsetCommitRequestOpt;
        // first update the acks pending variable according to the error code;
        partitionStatus.forEach((topicAndPartition, delayedStatus) -> {
            if (delayedStatus.responseStatus.error == ErrorMapping.NoError) {
                // Timeout error state will be cleared when required acks are received;
                delayedStatus.acksPending = true;
                delayedStatus.responseStatus.error = ErrorMapping.RequestTimedOutCode;
            } else {
                delayedStatus.acksPending = false;
            }

            trace(String.format("Initial partition status for %s is %s", topicAndPartition, delayedStatus));
        });
    }


    public RequestOrResponse respond(OffsetManager offsetManager) {
        Map<TopicAndPartition, ProducerResponseStatus> responseStatus = Utils.mapValue(partitionStatus, status -> status.responseStatus);
        Tuple<TopicAndPartition, ProducerResponseStatus> result = Utils.find(responseStatus, (k, v) -> v.error != ErrorMapping.NoError);
        Short errorCode = result.isEmpty() ? ErrorMapping.NoError : result.v2.error;
        if (errorCode == ErrorMapping.NoError) {
            Utils.foreach(offsetCommitRequestOpt, ocr -> offsetManager.putOffsets(ocr.groupId, ocr.requestInfo));
        }
        offsetCommitRequestOpt.orElseGet(null);
        Optional<RequestOrResponse> opt = Utils.map(offsetCommitRequestOpt, o -> o.responseFor(errorCode, offsetManager.config.maxMetadataSize));
        if (opt.isPresent()) {
            return opt.get();
        }
        return new ProducerResponse(produce.correlationId, responseStatus);
    }

    public boolean isSatisfied(ReplicaManager replicaManager) {
        // check for each partition if it still has pending acks;
        partitionStatus.forEach((topicAndPartition, fetchPartitionStatus) -> {
            trace(String.format("Checking producer request satisfaction for %s, acksPending = %b",
                    topicAndPartition, fetchPartitionStatus.acksPending));
            // skip those partitions that have already been satisfied;
            if (fetchPartitionStatus.acksPending) {
                Optional<Partition> partitionOpt = replicaManager.getPartition(topicAndPartition.topic, topicAndPartition.partition);
                Tuple<Boolean, Short> result = Utils.match(partitionOpt, partition -> partition.checkEnoughReplicasReachOffset(fetchPartitionStatus.requiredOffset, produce.requiredAcks.intValue())
                        , () -> Tuple.of(false, ErrorMapping.UnknownTopicOrPartitionCode));
                Boolean hasEnough = result.v1;
                Short errorCode = result.v2;
                if (errorCode != ErrorMapping.NoError) {
                    fetchPartitionStatus.acksPending = false;
                    fetchPartitionStatus.responseStatus.error = errorCode;
                } else if (hasEnough) {
                    fetchPartitionStatus.acksPending = false;
                    fetchPartitionStatus.responseStatus.error = ErrorMapping.NoError;
                }
            }
        });

        // unblocked if there are no partitions with pending acks;
        boolean satisfied = !Utils.exists(partitionStatus, (k, v) -> v.acksPending);
        return satisfied;
    }
}

class DelayedProduceResponseStatus {
    public Long requiredOffset;
    public ProducerResponseStatus responseStatus;
    public volatile boolean acksPending = false;

    public DelayedProduceResponseStatus(Long requiredOffset, ProducerResponseStatus responseStatus) {
        this.requiredOffset = requiredOffset;
        this.responseStatus = responseStatus;
    }

    @Override
    public String toString() {
        return String.format("acksPending:%b, error: %d, startOffset: %d, requiredOffset: %d",
                acksPending, responseStatus.error, responseStatus.offset, requiredOffset);
    }

}
