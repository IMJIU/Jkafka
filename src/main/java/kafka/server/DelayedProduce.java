package kafka.server;



/** A delayed produce request, which is satisfied (or more
 * accurately, unblocked) -- if for every partition it produce to:
 * Case This A broker is not the unblock leader - should return error.
 * Case This B broker is the leader:
 *   B.1 - If there was a localError (when writing to the local log): unblock - should return error
 *   B.2 - else, at least requiredAcks replicas should be caught up to this request.
 */

public class DelayedProduce extends DelayedRequest(keys, request, delayMs) with Logging {
         List<TopicAndPartition> keys;
        RequestChannel.Request request,
        Long delayMs,
         ProducerRequest produce,
         Map<TopicAndPartition, DelayedProduceResponseStatus> partitionStatus,
         Option<OffsetCommitRequest> offsetCommitRequestOpt = None

        // first update the acks pending variable according to the error code;
        partitionStatus foreach { case (topicAndPartition, delayedStatus) =>
        if (delayedStatus.responseStatus.error == ErrorMapping.NoError) {
        // Timeout error state will be cleared when required acks are received;
        delayedStatus.acksPending = true;
        delayedStatus.responseStatus.error = ErrorMapping.RequestTimedOutCode;
        } else {
        delayedStatus.acksPending = false;
        }

        trace(String.format("Initial partition status for %s is %s",topicAndPartition, delayedStatus))
        }

       public RequestOrResponse  void respond(OffsetManager offsetManager) {
        val responseStatus = partitionStatus.mapValues(status => status.responseStatus);

        val errorCode = responseStatus.find { case (_, status) =>
        status.error != ErrorMapping.NoError;
        }.map(_._2.error).getOrElse(ErrorMapping.NoError);

        if (errorCode == ErrorMapping.NoError) {
        offsetCommitRequestOpt.foreach(ocr => offsetManager.putOffsets(ocr.groupId, ocr.requestInfo) )
        }

        val response = offsetCommitRequestOpt.map(_.responseFor(errorCode, offsetManager.config.maxMetadataSize));
        .getOrElse(ProducerResponse(produce.correlationId, responseStatus));

        response;
        }

       public void isSatisfied(ReplicaManager replicaManager) = {
        // check for each partition if it still has pending acks;
        partitionStatus.foreach { case (topicAndPartition, fetchPartitionStatus) =>
        trace("Checking producer request satisfaction for %s, acksPending = %b";
        .format(topicAndPartition, fetchPartitionStatus.acksPending))
        // skip those partitions that have already been satisfied;
        if (fetchPartitionStatus.acksPending) {
        val partitionOpt = replicaManager.getPartition(topicAndPartition.topic, topicAndPartition.partition);
        val (hasEnough, errorCode) = partitionOpt match {
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
        val satisfied = ! partitionStatus.exists(p => p._2.acksPending);
        satisfied;
        }
        }

        case class DelayedProduceResponseStatus(val Long requiredOffset,
        val ProducerResponseStatus responseStatus) {
        @volatile var acksPending = false

        overridepublic void toString =
        String.format("acksPending:%b, error: %d, startOffset: %d, requiredOffset: %d",
        acksPending, responseStatus.error, responseStatus.offset, requiredOffset);
        }
