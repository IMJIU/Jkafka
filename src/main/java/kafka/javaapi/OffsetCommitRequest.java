package kafka.javaapi;

/**
 * @author zhoulf
 * @create 2017-12-19 20:16
 **/

class OffsetCommitRequest(groupId: String,
        requestInfo: java.util.Map[TopicAndPartition, OffsetAndMetadata],
        correlationId: Int,
        clientId: String,
        versionId: Short) {
        val underlying = {
        val scalaMap: collection.immutable.Map[TopicAndPartition, OffsetAndMetadata] = {
        import collection.JavaConversions._

        requestInfo.toMap
        }
        kafka.api.OffsetCommitRequest(
        groupId = groupId,
        requestInfo = scalaMap,
        versionId = versionId,
        correlationId = correlationId,
        clientId = clientId
        )
        }

        def this(groupId: String,
        requestInfo: java.util.Map[TopicAndPartition, OffsetAndMetadata],
        correlationId: Int,
        clientId: String) {

        // by default bind to version 0 so that it commits to Zookeeper
        this(groupId, requestInfo, correlationId, clientId, 0)
        }


        override def toString = underlying.toString


        override def equals(other: Any) = canEqual(other) && {
        val otherOffsetRequest = other.asInstanceOf[kafka.javaapi.OffsetCommitRequest]
        this.underlying.equals(otherOffsetRequest.underlying)
        }


        def canEqual(other: Any) = other.isInstanceOf[kafka.javaapi.OffsetCommitRequest]


        override def hashCode = underlying.hashCode

        }