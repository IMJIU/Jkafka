package kafka.javaapi;

import kafka.api.RequestKeys;
import kafka.api.RequestOrResponse;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * @author zhoulf
 * @create 2017-12-19 18 20
 **/

public class TopicMetadataRequest extends RequestOrResponse {
    public Short versionId;
    public Integer correlationId;
    public String clientId;
    public List<String> topics;
    public kafka.api.TopicMetadataRequest underlying;

    public TopicMetadataRequest(Short versionId, java.lang.Integer correlationId, String clientId, List<String> topics) {
        super(Optional.of(RequestKeys.MetadataKey));
        this.versionId = versionId;
        this.correlationId = correlationId;
        this.clientId = clientId;
        this.topics = topics;
        underlying = new kafka.api.TopicMetadataRequest(versionId, correlationId, clientId, topics);
    }


    public TopicMetadataRequest(List<String> topics) {
        this(kafka.api.TopicMetadataRequest.CurrentVersion, 0, kafka.api.TopicMetadataRequest.DefaultClientId, topics);
    }


    public TopicMetadataRequest(List<String> topics, Integer correlationId) {
        this(kafka.api.TopicMetadataRequest.CurrentVersion, correlationId, kafka.api.TopicMetadataRequest.DefaultClientId, topics);
    }


    public void writeTo(ByteBuffer buffer) {
        underlying.writeTo(buffer);
    }

    public Integer sizeInBytes() {
        return underlying.sizeInBytes();
    }

    @Override
    public String toString() {
        return describe(true);
    }

    @Override
    public String describe(Boolean details) {
        StringBuilder topicMetadataRequest = new StringBuilder();
        topicMetadataRequest.append("Name: " + this.getClass().getSimpleName());
        topicMetadataRequest.append("; Version: " + versionId);
        topicMetadataRequest.append("; CorrelationId: " + correlationId);
        topicMetadataRequest.append("; ClientId: " + clientId);
        if (details) {
            topicMetadataRequest.append("; Topics: ");
            Iterator<String> topicIterator = topics.iterator();
            while (topicIterator.hasNext()) {
                String topic = topicIterator.next();
                topicMetadataRequest.append(String.format("%s", topic));
                if (topicIterator.hasNext())
                    topicMetadataRequest.append(",");
            }
        }
        return topicMetadataRequest.toString();
    }
}
