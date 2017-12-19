package kafka.javaapi;

/**
 * @author zhoulf
 * @create 2017-12-19 20:16
 **/

class OffsetCommitResponse(private val underlying: kafka.api.OffsetCommitResponse) {

        def errors: java.util.Map[TopicAndPartition, Short] = {
        import JavaConversions._
        underlying.commitStatus
        }

        def hasError = underlying.hasError

        def errorCode(topicAndPartition: TopicAndPartition) = underlying.commitStatus(topicAndPartition)

        }

        object OffsetCommitResponse {
        def readFrom(buffer: ByteBuffer) = new OffsetCommitResponse(kafka.api.OffsetCommitResponse.readFrom(buffer))
        }