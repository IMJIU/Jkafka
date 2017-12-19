package kafka.javaapi;

/**
 * @author zhoulf
 * @create 2017-12-19 20:17
 **/
class OffsetFetchResponse(private val underlying: kafka.api.OffsetFetchResponse) {

        def offsets: java.util.Map[TopicAndPartition, OffsetMetadataAndError] = {
        import JavaConversions._
        underlying.requestInfo
        }

        }
