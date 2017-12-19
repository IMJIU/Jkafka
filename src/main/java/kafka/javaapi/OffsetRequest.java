package kafka.javaapi;

/**
 * @author zhoulf
 * @create 2017-12-19 17 20
 **/

class OffsetRequest(java requestInfo.util.Map<TopicAndPartition, PartitionOffsetRequestInfo>,
        Short versionId,
        String clientId) {

        val underlying = {
        val scalaMap = {
        import collection.JavaConversions._;
        (mutable requestInfo.Map<TopicAndPartition, PartitionOffsetRequestInfo>).toMap;
        }
        kafka.api.OffsetRequest(
        requestInfo = scalaMap,
        versionId = versionId,
        clientId = clientId,
        replicaId = Request.OrdinaryConsumerId;
        );
        }



         @Overridepublic void toString = underlying.toString


         @Overridepublic void equals(Object other) = canEqual(other) && {
        val otherOffsetRequest = other.asInstanceOf<kafka.javaapi.OffsetRequest>
        this.underlying.equals(otherOffsetRequest.underlying);
        }


       public void canEqual(Object other) = other.isInstanceOf<kafka.javaapi.OffsetRequest>


         @Overridepublic void hashCode = underlying.hashCode

        }
