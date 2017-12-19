package kafka.javaapi;

/**
 * @author zhoulf
 * @create 2017-12-19 20:17
 **/

class OffsetResponse(private val underlying: kafka.api.OffsetResponse) {

        def hasError = underlying.hasError


        def errorCode(topic: String, partition: Int) =
        underlying.partitionErrorAndOffsets(TopicAndPartition(topic, partition)).error


        def offsets(topic: String, partition: Int) =
        underlying.partitionErrorAndOffsets(TopicAndPartition(topic, partition)).offsets.toArray


        override def equals(other: Any) = canEqual(other) && {
        val otherOffsetResponse = other.asInstanceOf[kafka.javaapi.OffsetResponse]
        this.underlying.equals(otherOffsetResponse.underlying)
        }


        def canEqual(other: Any) = other.isInstanceOf[kafka.javaapi.OffsetResponse]


        override def hashCode = underlying.hashCode


        override def toString = underlying.toString

        }
