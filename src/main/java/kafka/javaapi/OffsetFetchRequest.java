package kafka.javaapi;

/**
 * @author zhoulf
 * @create 2017-12-19 20:17
 **/

class OffsetFetchRequest(groupId: String,
        requestInfo: java.util.List[TopicAndPartition],
        versionId: Short,
        correlationId: Int,
        clientId: String) {

        def this(groupId: String,
        requestInfo: java.util.List[TopicAndPartition],
        correlationId: Int,
        clientId: String) {
        // by default bind to version 0 so that it fetches from ZooKeeper
        this(groupId, requestInfo, 0, correlationId, clientId)
        }

        val underlying = {
        val scalaSeq = {
        import JavaConversions._
        requestInfo: mutable.Buffer[TopicAndPartition]
        }
        kafka.api.OffsetFetchRequest(
        groupId = groupId,
        requestInfo = scalaSeq,
        versionId = versionId,
        correlationId = correlationId,
        clientId = clientId
        )
        }


        override def toString = underlying.toString


        override def equals(other: Any) = canEqual(other) && {
        val otherOffsetRequest = other.asInstanceOf[kafka.javaapi.OffsetFetchRequest]
        this.underlying.equals(otherOffsetRequest.underlying)
        }


        def canEqual(other: Any) = other.isInstanceOf[kafka.javaapi.OffsetFetchRequest]


        override def hashCode = underlying.hashCode

        }