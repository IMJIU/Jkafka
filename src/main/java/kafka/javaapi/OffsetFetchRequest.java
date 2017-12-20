package kafka.javaapi;

import kafka.log.TopicAndPartition;

import java.util.List;

/**
 * @author zhoulf
 * @create 2017-12-19 17 20
 **/

public class OffsetFetchRequest {
    public String groupId;
    public List<TopicAndPartition> requestInfo;
    public Short versionId;
    public Integer correlationId;
    public String clientId;

    public OffsetFetchRequest(String groupId, List<TopicAndPartition> requestInfo, Short versionId, Integer correlationId, String clientId) {
        this.groupId = groupId;
        this.requestInfo = requestInfo;
        this.versionId = versionId;
        this.correlationId = correlationId;
        this.clientId = clientId;
    }

    public OffsetFetchRequest(String groupId,
                              List<TopicAndPartition> requestInfo,
                              Integer correlationId,
                              String clientId) {
        // by default bind to version 0 so that it fetches from ZooKeeper;
        this(groupId, requestInfo, (short) 0, correlationId, clientId);
        List<TopicAndPartition> scalaSeq = requestInfo;
        underlying = new kafka.api.OffsetFetchRequest(groupId, scalaSeq, versionId, correlationId, clientId);
    }

    public kafka.api.OffsetFetchRequest underlying;


    @Override
    public String toString() {
        return underlying.toString();
    }


    @Override
    public boolean equals(Object other) {
        return canEqual(other) && this.underlying.equals(((kafka.javaapi.OffsetFetchRequest) other).underlying);
    }


    public boolean canEqual(Object other) {
        return other instanceof kafka.javaapi.OffsetFetchRequest;
    }


    @Override
    public int hashCode() {
        return underlying.hashCode();
    }

}
