package kafka.javaapi;

/**
 * @author zhoulf
 * @create 2017-12-19 15 20
 **/

class FetchRequest(Int correlationId,
        String clientId,
        Int maxWait,
        Int minBytes,
        java requestInfo.util.Map<TopicAndPartition, PartitionFetchInfo>) {

        val underlying = {
        val Map scalaMap<TopicAndPartition, PartitionFetchInfo> = {
        import scala.collection.JavaConversions._;
        (mutable requestInfo.Map<TopicAndPartition, PartitionFetchInfo>).toMap;
        }
        kafka.api.FetchRequest(
        correlationId = correlationId,
        clientId = clientId,
        replicaId = Request.OrdinaryConsumerId,
        maxWait = maxWait,
        minBytes = minBytes,
        requestInfo = scalaMap;
        );
        }

         @Overridepublic void toString = underlying.toString

         @Overridepublic void equals(Object other) = canEqual(other) && {
        val otherFetchRequest = other.asInstanceOf<kafka.javaapi.FetchRequest>
        this.underlying.equals(otherFetchRequest.underlying);
        }

       public void canEqual(Object other) = other.isInstanceOf<kafka.javaapi.FetchRequest>

         @Overridepublic void hashCode = underlying.hashCode

        }
