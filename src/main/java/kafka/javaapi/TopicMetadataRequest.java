package kafka.javaapi;

/**
 * @author zhoulf
 * @create 2017-12-19 20:18
 **/

class TopicMetadataRequest(val versionId: Short,
        val correlationId: Int,
        val clientId: String,
        val topics: java.util.List[String])
        extends RequestOrResponse(Some(kafka.api.RequestKeys.MetadataKey)) {

        val underlying: kafka.api.TopicMetadataRequest = {
        import scala.collection.JavaConversions._
        new kafka.api.TopicMetadataRequest(versionId, correlationId, clientId, topics: mutable.Buffer[String])
        }

        def this(topics: java.util.List[String]) =
        this(kafka.api.TopicMetadataRequest.CurrentVersion, 0, kafka.api.TopicMetadataRequest.DefaultClientId, topics)

        def this(topics: java.util.List[String], correlationId: Int) =
        this(kafka.api.TopicMetadataRequest.CurrentVersion, correlationId, kafka.api.TopicMetadataRequest.DefaultClientId, topics)

        def writeTo(buffer: ByteBuffer) = underlying.writeTo(buffer)

        def sizeInBytes: Int = underlying.sizeInBytes()

        override def toString(): String = {
        describe(true)
        }

        override def describe(details: Boolean): String = {
        val topicMetadataRequest = new StringBuilder
        topicMetadataRequest.append("Name: " + this.getClass.getSimpleName)
        topicMetadataRequest.append("; Version: " + versionId)
        topicMetadataRequest.append("; CorrelationId: " + correlationId)
        topicMetadataRequest.append("; ClientId: " + clientId)
        if(details) {
        topicMetadataRequest.append("; Topics: ")
        val topicIterator = topics.iterator()
        while (topicIterator.hasNext) {
        val topic = topicIterator.next()
        topicMetadataRequest.append("%s".format(topic))
        if(topicIterator.hasNext)
        topicMetadataRequest.append(",")
        }
        }
        topicMetadataRequest.toString()
        }
        }
