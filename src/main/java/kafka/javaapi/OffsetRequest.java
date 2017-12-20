package kafka.javaapi;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.api.Request;
import kafka.log.TopicAndPartition;

import java.util.Map;

/**
 * @author zhoulf
 * @create 2017-12-19 17 20
 **/

public class OffsetRequest {
    public Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo;
    public Short versionId;
    public String clientId;

    public OffsetRequest(Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo, Short versionId, String clientId) {
        this.requestInfo = requestInfo;
        this.versionId = versionId;
        this.clientId = clientId;
        underlying = new kafka.api.OffsetRequest(requestInfo, versionId, 0, clientId, Request.OrdinaryConsumerId);
    }

    public kafka.api.OffsetRequest underlying;


    @Override
    public String toString() {
        return underlying.toString();
    }


    @Override
    public boolean equals(Object other) {
        return canEqual(other) && this.underlying.equals(((kafka.javaapi.OffsetRequest) other).underlying);
    }


    public boolean canEqual(Object other) {
        return other instanceof kafka.javaapi.OffsetRequest;
    }

    @Override
    public int hashCode() {
        return underlying.hashCode();
    }

}
