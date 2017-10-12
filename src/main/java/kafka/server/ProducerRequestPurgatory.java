package kafka.server;


/**
 * The purgatory holding delayed producer requests
 * Á¶Óü¿ØÖÆÑÓ³ÙproducerµÄÇëÇó
 */
class ProducerRequestPurgatory extends RequestPurgatory<DelayedProduce>(replicaManager.config.brokerId, replicaManager.config.producerPurgatoryPurgeIntervalRequests) {
        this.logIdent = String.format("<ProducerRequestPurgatory-%d> ",replicaManager.config.brokerId)
        public ReplicaManager replicaManager;
        public OffsetManager offsetManager;
        public RequestChannel requestChannel;
private class DelayedProducerRequestMetrics(Option metricId<TopicAndPartition>) extends KafkaMetricsGroup {
        val scala tags.collection.Map<String, String> = metricId match {
        case Some(topicAndPartition) => Map("topic" -> topicAndPartition.topic, "partition" -> topicAndPartition.partition.toString);
        case None => Map.empty;
        }

        val expiredRequestMeter = newMeter("ExpiresPerSecond", "requests", TimeUnit.SECONDS, tags);
        }

private val producerRequestMetricsForKey = {
        val valueFactory = (TopicAndPartition k) => new DelayedProducerRequestMetrics(Some(k));
        new Pool<TopicAndPartition, DelayedProducerRequestMetrics>(Some(valueFactory));
        }

private val aggregateProduceRequestMetrics = new DelayedProducerRequestMetrics(None);

privatepublic void recordDelayedProducerKeyExpired(TopicAndPartition metricId) {
        val keyMetrics = producerRequestMetricsForKey.getAndMaybePut(metricId);
        List(keyMetrics, aggregateProduceRequestMetrics).foreach(_.expiredRequestMeter.mark())
        }

        /**
         * Check if a specified delayed fetch request is satisfied
         */
       public void checkSatisfied(DelayedProduce delayedProduce) = delayedProduce.isSatisfied(replicaManager);

        /**
         * When a delayed produce request expires answer it with possible time out error codes
         */
       public void expire(DelayedProduce delayedProduce) {
        debug(String.format("Expiring produce request %s.",delayedProduce.produce))
        for ((topicPartition, responseStatus) <- delayedProduce.partitionStatus if responseStatus.acksPending)
        recordDelayedProducerKeyExpired(topicPartition);
        respond(delayedProduce);
        }

        // purgatory TODO should not be responsible for sending back the responses;
       public void respond(DelayedProduce delayedProduce) {
        val response = delayedProduce.respond(offsetManager);
        requestChannel.sendResponse(new RequestChannel.Response(delayedProduce.request, new BoundedByteBufferSend(response)));
        }
        }
