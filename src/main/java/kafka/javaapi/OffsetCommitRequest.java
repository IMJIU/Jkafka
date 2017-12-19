package kafka.javaapi;

/**
 * @author zhoulf
 * @create 2017-12-19 16 20
 **/

class OffsetCommitRequest(String groupId,
        java requestInfo.util.Map<TopicAndPartition, OffsetAndMetadata>,
        Int correlationId,
        String clientId,
        Short versionId) {
        val underlying = {
        val collection scalaMap.immutable.Map<TopicAndPartition, OffsetAndMetadata> = {
        import collection.JavaConversions._;

        requestInfo.toMap;
        }
        kafka.api.OffsetCommitRequest(
        groupId = groupId,
        requestInfo = scalaMap,
        versionId = versionId,
        correlationId = correlationId,
        clientId = clientId;
        );
        }

       public void this(String groupId,
        java requestInfo.util.Map<TopicAndPartition, OffsetAndMetadata>,
        Int correlationId,
        String clientId) {

        // by default bind to version 0 so that it commits to Zookeeper;
        this(groupId, requestInfo, correlationId, clientId, 0);
        }


         @Overridepublic void toString = underlying.toString


         @Overridepublic void equals(Object other) = canEqual(other) && {
        val otherOffsetRequest = other.asInstanceOf<kafka.javaapi.OffsetCommitRequest>
        this.underlying.equals(otherOffsetRequest.underlying);
        }


       public void canEqual(Object other) = other.isInstanceOf<kafka.javaapi.OffsetCommitRequest>


         @Overridepublic void hashCode = underlying.hashCode

        }
