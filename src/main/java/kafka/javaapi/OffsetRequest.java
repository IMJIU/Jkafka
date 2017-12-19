package kafka.javaapi;

/**
 * @author zhoulf
 * @create 2017-12-19 20:17
 **/

class OffsetRequest(requestInfo: java.util.Map[TopicAndPartition, PartitionOffsetRequestInfo],
        versionId: Short,
        clientId: String) {

        val underlying = {
        val scalaMap = {
        import collection.JavaConversions._
        (requestInfo: mutable.Map[TopicAndPartition, PartitionOffsetRequestInfo]).toMap
        }
        kafka.api.OffsetRequest(
        requestInfo = scalaMap,
        versionId = versionId,
        clientId = clientId,
        replicaId = Request.OrdinaryConsumerId
        )
        }



        override def toString = underlying.toString


        override def equals(other: Any) = canEqual(other) && {
        val otherOffsetRequest = other.asInstanceOf[kafka.javaapi.OffsetRequest]
        this.underlying.equals(otherOffsetRequest.underlying)
        }


        def canEqual(other: Any) = other.isInstanceOf[kafka.javaapi.OffsetRequest]


        override def hashCode = underlying.hashCode

        }
