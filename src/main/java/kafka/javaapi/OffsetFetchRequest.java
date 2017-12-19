package kafka.javaapi;

/**
 * @author zhoulf
 * @create 2017-12-19 17 20
 **/

class OffsetFetchRequest(String groupId,
        java requestInfo.util.List<TopicAndPartition>,
        Short versionId,
        Int correlationId,
        String clientId) {

       public void this(String groupId,
        java requestInfo.util.List<TopicAndPartition>,
        Int correlationId,
        String clientId) {
        // by default bind to version 0 so that it fetches from ZooKeeper;
        this(groupId, requestInfo, 0, correlationId, clientId);
        }

        val underlying = {
        val scalaSeq = {
        import JavaConversions._;
        mutable requestInfo.Buffer<TopicAndPartition>
        }
        kafka.api.OffsetFetchRequest(
        groupId = groupId,
        requestInfo = scalaSeq,
        versionId = versionId,
        correlationId = correlationId,
        clientId = clientId;
        );
        }


         @Overridepublic void toString = underlying.toString


         @Overridepublic void equals(Object other) = canEqual(other) && {
        val otherOffsetRequest = other.asInstanceOf<kafka.javaapi.OffsetFetchRequest>
        this.underlying.equals(otherOffsetRequest.underlying);
        }


       public void canEqual(Object other) = other.isInstanceOf<kafka.javaapi.OffsetFetchRequest>


         @Overridepublic void hashCode = underlying.hashCode

        }
