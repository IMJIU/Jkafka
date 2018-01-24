package kafka.api;

import kafka.common.ErrorMapping;
import kafka.common.OffsetAndMetadata;
import kafka.common.OffsetMetadataAndError;
import kafka.func.Handler;
import kafka.func.IntCount;
import kafka.func.Tuple;
import kafka.log.TopicAndPartition;
import kafka.network.BoundedByteBufferSend;
import kafka.network.RequestChannel;
import kafka.utils.Logging;
import kafka.utils.Sc;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static kafka.api.ApiUtils.*;

/**
 * @author zhoulf
 * @create 2017-10-30 34 13
 **/
public class OffsetFetchRequest extends RequestOrResponse {//(Some(RequestKeys.OffsetFetchKey))
    // version 0 and 1 have exactly the same wire format, but different functionality.;
    // version 0 will read the offsets from ZK and version 1 will read the offsets from Kafka.;
    public static final Short CurrentVersion = 1;
    public static final String DefaultClientId = "";
    public String groupId;
    public List<TopicAndPartition> requestInfo;
    public Short versionId = OffsetFetchRequest.CurrentVersion;
    public Integer correlationId = 0;
    public String clientId = OffsetFetchRequest.DefaultClientId;
    // TODO: 2017/10/30 lazy
    private Map<String, List<TopicAndPartition>> requestInfoGroupedByTopic;

    public Map<String, List<TopicAndPartition>> requestInfoGroupedByTopic() {
        if (requestInfoGroupedByTopic == null) {
            requestInfoGroupedByTopic = Sc.groupBy(requestInfo, r -> r.topic);
        }
        return requestInfoGroupedByTopic;
    }

    public OffsetFetchRequest(java.lang.String groupId, List<TopicAndPartition> requestInfo, java.lang.String clientId) {
        this(groupId, requestInfo, OffsetFetchRequest.CurrentVersion, 0, clientId);
    }

    public OffsetFetchRequest(java.lang.String groupId, List<TopicAndPartition> requestInfo, Short versionId, Integer correlationId, java.lang.String clientId) {
        super(Optional.of(RequestKeys.OffsetFetchKey));
        this.groupId = groupId;
        this.requestInfo = requestInfo;
        this.versionId = versionId;
        this.correlationId = correlationId;
        this.clientId = clientId;
    }

    public final static Handler<ByteBuffer, OffsetFetchRequest> readFrom = (buffer) -> {
        // Read values from the envelope;
        short versionId = buffer.getShort();
        int correlationId = buffer.getInt();
        String clientId = readShortString(buffer);

        // Read the OffsetFetchRequest;
        String consumerGroupId = readShortString(buffer);
        int topicCount = buffer.getInt();
        List<TopicAndPartition> pairs = Sc.itFlatToList(1, topicCount, n -> {
            String topic = readShortString(buffer);
            int partitionCount = buffer.getInt();
            return Sc.itToList(1, partitionCount, m -> {
                int partitionId = buffer.getInt();
                return new TopicAndPartition(topic, partitionId);
            }).stream();
        });
        return new OffsetFetchRequest(consumerGroupId, pairs, versionId, correlationId, clientId);
    };


    public void writeTo(ByteBuffer buffer) {
        // Write envelope;
        buffer.putShort(versionId);
        buffer.putInt(correlationId);
        writeShortString(buffer, clientId);

        // Write OffsetFetchRequest;
        writeShortString(buffer, groupId);        // consumer group;
        buffer.putInt(requestInfoGroupedByTopic.size()); // number of topics;
        requestInfoGroupedByTopic.forEach((topic, list) -> { // (topic, Seq<TopicAndPartition>)
            writeShortString(buffer, topic); // topic;
            buffer.putInt(list.size());      // number of partitions for this topic;
            list.forEach(t2 -> buffer.putInt(t2.partition));
        });
    }

    @Override
    public Integer sizeInBytes() {
        IntCount size = IntCount.of(
                2 + /* versionId */
                        4 + /* correlationId */
                        shortStringLength(clientId) +
                        shortStringLength(groupId) +
                        4 /* topic count */);
        requestInfoGroupedByTopic.forEach((topic, list) ->
                size.add(shortStringLength(topic) + /* topic */
                        4 + /* number of partitions */
                        list.size() * 4 /* partition */)
        );
        return size.get();
    }

    @Override
    public void handleError(Throwable e, RequestChannel requestChannel, RequestChannel.Request request) {
        Map<TopicAndPartition, OffsetMetadataAndError> responseMap = Sc.toMap(Sc.map(requestInfo, topicAndPartition ->
                Tuple.of(topicAndPartition,
                        new OffsetMetadataAndError(OffsetAndMetadata.InvalidOffset, ErrorMapping.codeFor(e.getClass())))));
        OffsetFetchResponse errorResponse = new OffsetFetchResponse(responseMap, correlationId = correlationId);
        requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(errorResponse)));
    }

    @Override
    public String describe(Boolean details) {
        StringBuilder offsetFetchRequest = new StringBuilder();
        offsetFetchRequest.append("Name: " + this.getClass().getSimpleName());
        offsetFetchRequest.append("; Version: " + versionId);
        offsetFetchRequest.append("; CorrelationId: " + correlationId);
        offsetFetchRequest.append("; ClientId: " + clientId);
        offsetFetchRequest.append("; GroupId: " + groupId);
        if (details)
            offsetFetchRequest.append("; RequestInfo: " + requestInfo);
        return offsetFetchRequest.toString();
    }

    @Override
    public String toString() {
        return describe(true);
    }
}
