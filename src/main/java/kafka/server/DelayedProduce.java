package kafka.server;


import kafka.api.OffsetCommitRequest;
import kafka.api.ProducerRequest;
import kafka.api.ProducerResponseStatus;
import kafka.api.RequestOrResponse;
import kafka.common.ErrorMapping;
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
        Utils.find(responseStatus,(k,v)->{
            v.error != ErrorMapping.NoError;
        })
        val errorCode = responseStatus.find {
            case (_,status) =>
                status.error != ErrorMapping.NoError;
        }.map(_._2.error).getOrElse(ErrorMapping.NoError);

        if (errorCode == ErrorMapping.NoError) {
            offsetCommitRequestOpt.foreach(ocr = > offsetManager.putOffsets(ocr.groupId, ocr.requestInfo) )
        }

        val response = offsetCommitRequestOpt.map(_.responseFor(errorCode, offsetManager.config.maxMetadataSize));
        .getOrElse(ProducerResponse(produce.correlationId, responseStatus));

        return response;
    }

    public boolean isSatisfied(ReplicaManager replicaManager) {
        // check for each partition if it still has pending acks;
        partitionStatus.foreach {
            case (topicAndPartition,fetchPartitionStatus) =>
                trace("Checking producer request satisfaction for %s, acksPending = %b";
        .format(topicAndPartition, fetchPartitionStatus.acksPending))
                // skip those partitions that have already been satisfied;
                if (fetchPartitionStatus.acksPending) {
                    val partitionOpt = replicaManager.getPartition(topicAndPartition.topic, topicAndPartition.partition);
                    val(hasEnough, errorCode) = partitionOpt match {
                        case Some(partition) =>
                            partition.checkEnoughReplicasReachOffset(
                                    fetchPartitionStatus.requiredOffset,
                                    produce.requiredAcks);
                        case None =>
                            (false, ErrorMapping.UnknownTopicOrPartitionCode);
                    }
                    if (errorCode != ErrorMapping.NoError) {
                        fetchPartitionStatus.acksPending = false;
                        fetchPartitionStatus.responseStatus.error = errorCode;
                    } else if (hasEnough) {
                        fetchPartitionStatus.acksPending = false;
                        fetchPartitionStatus.responseStatus.error = ErrorMapping.NoError;
                    }
                }
        }

        // unblocked if there are no partitions with pending acks;
        boolean satisfied = !partitionStatus.exists(p = > p._2.acksPending);
        return satisfied;
    }
}

class DelayedProduceResponseStatus {
    public Long requiredOffset;
    public ProducerResponseStatus responseStatus;
    public volatile boolean acksPending = false;

    @Override
    public String toString() {
        return String.format("acksPending:%b, error: %d, startOffset: %d, requiredOffset: %d",
                acksPending, responseStatus.error, responseStatus.offset, requiredOffset);
    }

}
