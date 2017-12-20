package kafka.javaapi;

import kafka.api.PartitionFetchInfo;
import kafka.api.Request;
import kafka.log.TopicAndPartition;

import java.util.Map;

/**
 * @author zhoulf
 * @create 2017-12-19 15 20
 **/

public class FetchRequest {
    public Integer correlationId;
    public String clientId;
    public Integer maxWait;
    public Integer minBytes;
    public Map<TopicAndPartition, PartitionFetchInfo> requestInfo;
    public kafka.api.FetchRequest underlying;

    public FetchRequest(Integer correlationId, String clientId, Integer maxWait, Integer minBytes, Map<TopicAndPartition, PartitionFetchInfo> requestInfo) {
        this.correlationId = correlationId;
        this.clientId = clientId;
        this.maxWait = maxWait;
        this.minBytes = minBytes;
        this.requestInfo = requestInfo;
        Map<TopicAndPartition, PartitionFetchInfo> scalaMap = requestInfo;
        underlying = new kafka.api.FetchRequest(kafka.api.FetchRequest.CurrentVersion, correlationId, clientId, Request.OrdinaryConsumerId, maxWait, minBytes, scalaMap);
    }

    @Override
    public String toString() {
        return underlying.toString();
    }

    @Override
    public boolean equals(Object other) {
        return canEqual(other) && this.underlying.equals(((kafka.javaapi.FetchRequest) other).underlying);
    }


    public boolean canEqual(Object other) {
        return other instanceof kafka.javaapi.FetchRequest;
    }

    @Override
    public int hashCode() {
        return underlying.hashCode();
    }

}
