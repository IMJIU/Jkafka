package kafka.javaapi;

/**
 * @author zhoulf
 * @create 2017-12-19 16 20
 **/

class OffsetCommitResponse(private val kafka underlying.api.OffsetCommitResponse) {

       public void java errors.util.Map<TopicAndPartition, Short> = {
        import JavaConversions._;
        underlying.commitStatus;
        }

       public void hasError = underlying.hasError;

       public void errorCode(TopicAndPartition topicAndPartition) = underlying.commitStatus(topicAndPartition);

        }

        object OffsetCommitResponse {
       public void readFrom(ByteBuffer buffer) = new OffsetCommitResponse(kafka.api.OffsetCommitResponse.readFrom(buffer));
        }
