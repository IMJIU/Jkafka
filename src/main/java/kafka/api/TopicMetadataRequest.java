package kafka.api;

import com.google.common.collect.Lists;
import kafka.common.ErrorMapping;
import kafka.func.Handler;
import kafka.func.IntCount;
import kafka.func.Tuple;
import kafka.network.BoundedByteBufferSend;
import kafka.network.RequestChannel;
import kafka.utils.Logging;
import kafka.utils.Sc;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static kafka.api.ApiUtils.*;

/**
 * @author zhoulf
 * @create 2017-10-27 35 17
 **/

public class TopicMetadataRequest extends RequestOrResponse {// (Some(RequestKeys.MetadataKey)){
    public static final short CurrentVersion = 0;
    public static final String DefaultClientId = "";
    public Short versionId;
    public Integer correlationId;
    public String clientId;
    public List<String> topics;

    public TopicMetadataRequest(Short versionId, Integer correlationId, String clientId, List<String> topics) {
        super(Optional.of(RequestKeys.MetadataKey));
        this.versionId = versionId;
        this.correlationId = correlationId;
        this.clientId = clientId;
        this.topics = topics;
    }

    /**
     * TopicMetadataRequest has the following format -
     * number of topics (4 bytes) list of topics (2 bytes + topic.length per topic) detailedMetadata (2 bytes) timestamp (8 bytes) count (4 bytes)
     */
    public final static Handler<ByteBuffer, TopicMetadataRequest> readFrom = (buffer) -> {
        short versionId = buffer.getShort();
        int correlationId = buffer.getInt();
        String clientId = readShortString(buffer);
        int numTopics = readIntInRange(buffer, "number of topics", Tuple.of(0, Integer.MAX_VALUE));
        List<String> topics = Lists.newArrayList();
        Sc.it(0, numTopics, n -> topics.add(readShortString(buffer)));
        return new TopicMetadataRequest(versionId, correlationId, clientId, topics);
    };


    public TopicMetadataRequest(List<String> topics, Integer correlationId) {
        this(TopicMetadataRequest.CurrentVersion, correlationId, TopicMetadataRequest.DefaultClientId, topics);
    }


    public void writeTo(ByteBuffer buffer) {
        buffer.putShort(versionId);
        buffer.putInt(correlationId);
        writeShortString(buffer, clientId);
        buffer.putInt(topics.size());
        topics.forEach(topic -> writeShortString(buffer, topic));
    }

    public Integer sizeInBytes() {
        IntCount size = IntCount.of(
                2 +  /* version id */
                        4 + /* correlation id */
                        shortStringLength(clientId) + /* client id */
                        4  /* number of topics */);
        topics.forEach(t -> size.add(shortStringLength(t)));/* topics */
        return size.get();
    }

    @Override
    public String toString() {
        return describe(true);
    }

    @Override
    public void handleError(Throwable e, RequestChannel requestChannel, RequestChannel.Request request) {
        List<TopicMetadata> topicMetadata = Sc.map(topics, topic -> new TopicMetadata(topic, null, ErrorMapping.codeFor(e.getClass())));
        TopicMetadataResponse errorResponse = new TopicMetadataResponse(Collections.emptyList(), topicMetadata, correlationId);
        requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(errorResponse)));
    }

    @Override
    public String describe(Boolean details) {
        StringBuilder topicMetadataRequest = new StringBuilder();
        topicMetadataRequest.append("Name: " + this.getClass().getSimpleName());
        topicMetadataRequest.append("; Version: " + versionId);
        topicMetadataRequest.append("; CorrelationId: " + correlationId);
        topicMetadataRequest.append("; ClientId: " + clientId);
        if (details)
            topicMetadataRequest.append("; Topics: " + topics);
        return topicMetadataRequest.toString();
    }
}
