package kafka.api;

import kafka.annotation.nonthreadsafe;
import kafka.consumer.ConsumerConfig;
import kafka.log.TopicAndPartition;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@nonthreadsafe
public class FetchRequestBuilder {
    private AtomicInteger correlationId = new AtomicInteger(0);
    private Short versionId = FetchRequest.CurrentVersion;
    private String clientId = ConsumerConfig.DefaultClientId;
    private Integer replicaId = Request.OrdinaryConsumerId;
    private Integer maxWait = FetchRequest.DefaultMaxWait;
    private Integer minBytes = FetchRequest.DefaultMinBytes;
    private Map<TopicAndPartition, PartitionFetchInfo> requestMap = new HashMap<>();

    public FetchRequestBuilder addFetch(String topic, Integer partition, Long offset, Integer fetchSize) {
        requestMap.put(new TopicAndPartition(topic, partition), new PartitionFetchInfo(offset, fetchSize));
        return this;
    }

    public FetchRequestBuilder clientId(String clientId) {
        this.clientId = clientId;
        return  this;
    }

    /**
     * Only for internal use. Clients shouldn't set replicaId.
     */
    public FetchRequestBuilder replicaId(Integer replicaId) {
        this.replicaId = replicaId;
        return this;
    }

    public FetchRequestBuilder maxWait(Integer maxWait) {
        this.maxWait = maxWait;
        return this;
    }

    public FetchRequestBuilder minBytes(Integer minBytes) {
        this.minBytes = minBytes;
        return this;
    }

    public FetchRequest build(){
        FetchRequest fetchRequest = new FetchRequest(versionId, correlationId.getAndIncrement(), clientId, replicaId, maxWait, minBytes, requestMap);
        requestMap.clear();
        return fetchRequest;
    }
}
