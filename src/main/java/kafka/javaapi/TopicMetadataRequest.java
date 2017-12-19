package kafka.javaapi;

/**
 * @author zhoulf
 * @create 2017-12-19 18 20
 **/

class TopicMetadataRequest(val Short versionId,
        val Int correlationId,
        val String clientId,
        val java topics.util.List<String>);
        extends RequestOrResponse(Optional.of(kafka.api.RequestKeys.MetadataKey)) {

        val kafka underlying.api.TopicMetadataRequest = {
        import scala.collection.JavaConversions._;
        new kafka.api.TopicMetadataRequest(versionId, correlationId, clientId, mutable topics.Buffer<String>);
        }

       public void this(java topics.util.List<String>) =
        this(kafka.api.TopicMetadataRequest.CurrentVersion, 0, kafka.api.TopicMetadataRequest.DefaultClientId, topics);

       public void this(java topics.util.List<String>, Int correlationId) =
        this(kafka.api.TopicMetadataRequest.CurrentVersion, correlationId, kafka.api.TopicMetadataRequest.DefaultClientId, topics);

       public void writeTo(ByteBuffer buffer) = underlying.writeTo(buffer);

       public void Integer sizeInBytes = underlying.sizeInBytes();

         @Overridepublic String  void toString() {
        describe(true);
        }

         @Overridepublic String  void describe(Boolean details) {
        val topicMetadataRequest = new StringBuilder;
        topicMetadataRequest.append("Name: " + this.getClass.getSimpleName);
        topicMetadataRequest.append("; Version: " + versionId);
        topicMetadataRequest.append("; CorrelationId: " + correlationId);
        topicMetadataRequest.append("; ClientId: " + clientId);
        if(details) {
        topicMetadataRequest.append("; Topics: ");
        val topicIterator = topics.iterator();
        while (topicIterator.hasNext) {
        val topic = topicIterator.next();
        topicMetadataRequest.append(String.format("%s",topic))
        if(topicIterator.hasNext)
        topicMetadataRequest.append(",");
        }
        }
        topicMetadataRequest.toString();
        }
        }
