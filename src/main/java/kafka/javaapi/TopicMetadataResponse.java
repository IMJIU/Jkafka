package kafka.javaapi;

/**
 * @author zhoulf
 * @create 2017-12-19 20:18
 **/

class TopicMetadataResponse(private val underlying: kafka.api.TopicMetadataResponse) {
        def sizeInBytes: Int = underlying.sizeInBytes

        def topicsMetadata: java.util.List[kafka.javaapi.TopicMetadata] = {
        import kafka.javaapi.MetadataListImplicits._
        underlying.topicsMetadata
        }

        override def equals(other: Any) = canEqual(other) && {
        val otherTopicMetadataResponse = other.asInstanceOf[kafka.javaapi.TopicMetadataResponse]
        this.underlying.equals(otherTopicMetadataResponse.underlying)
        }

        def canEqual(other: Any) = other.isInstanceOf[kafka.javaapi.TopicMetadataResponse]

        override def hashCode = underlying.hashCode

        override def toString = underlying.toString
        }