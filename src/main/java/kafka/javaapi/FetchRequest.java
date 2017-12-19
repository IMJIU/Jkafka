package kafka.javaapi;

/**
 * @author zhoulf
 * @create 2017-12-19 20:15
 **/

class FetchRequest(correlationId: Int,
        clientId: String,
        maxWait: Int,
        minBytes: Int,
        requestInfo: java.util.Map[TopicAndPartition, PartitionFetchInfo]) {

        val underlying = {
        val scalaMap: Map[TopicAndPartition, PartitionFetchInfo] = {
        import scala.collection.JavaConversions._
        (requestInfo: mutable.Map[TopicAndPartition, PartitionFetchInfo]).toMap
        }
        kafka.api.FetchRequest(
        correlationId = correlationId,
        clientId = clientId,
        replicaId = Request.OrdinaryConsumerId,
        maxWait = maxWait,
        minBytes = minBytes,
        requestInfo = scalaMap
        )
        }

        override def toString = underlying.toString

        override def equals(other: Any) = canEqual(other) && {
        val otherFetchRequest = other.asInstanceOf[kafka.javaapi.FetchRequest]
        this.underlying.equals(otherFetchRequest.underlying)
        }

        def canEqual(other: Any) = other.isInstanceOf[kafka.javaapi.FetchRequest]

        override def hashCode = underlying.hashCode

        }