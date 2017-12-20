package kafka.javaapi;

import kafka.common.OffsetAndMetadata;
import kafka.log.TopicAndPartition;

import java.util.Map;

/**
 * @author zhoulf
 * @create 2017-12-19 16 20
 **/

public class OffsetCommitRequest {
    public String groupId;
    public Map<TopicAndPartition, OffsetAndMetadata> requestInfo;
    public Integer correlationId;
    public String clientId;
    public Short versionId;
    public kafka.api.OffsetCommitRequest underlying;

    public OffsetCommitRequest(String groupId, Map<TopicAndPartition, OffsetAndMetadata> requestInfo, Integer correlationId, String clientId, Short versionId) {
        this.groupId = groupId;
        this.requestInfo = requestInfo;
        this.correlationId = correlationId;
        this.clientId = clientId;
        this.versionId = versionId;
    }

    public OffsetCommitRequest(String groupId,
                               Map<TopicAndPartition, OffsetAndMetadata> requestInfo,
                               Integer correlationId,
                               String clientId) {

        // by default bind to version 0 so that it commits to Zookeeper;
        this(groupId, requestInfo, correlationId, clientId, (short) 0);
    }


    @Override
    public String toString() {
        return underlying.toString();
    }


    @Override
    public boolean equals(Object other) {
        return canEqual(other) && this.underlying.equals(((kafka.javaapi.OffsetCommitRequest) other).underlying);
    }


    public boolean canEqual(Object other) {
        return other instanceof kafka.javaapi.OffsetCommitRequest;
    }

    @Override
    public int hashCode() {
        return underlying.hashCode();
    }

}
