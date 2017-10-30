package kafka.api;

import kafka.common.ErrorMapping;
import kafka.func.IntCount;
import kafka.func.Tuple;
import kafka.log.TopicAndPartition;
import kafka.network.BoundedByteBufferSend;
import kafka.network.RequestChannel;
import kafka.utils.Sc;

import java.nio.ByteBuffer;
import java.util.*;

import static kafka.api.ApiUtils.*;

/**
 * Created by Administrator on 2017/4/4.
 */
public class OffsetRequest extends RequestOrResponse {//RequestOrResponse(Some(RequestKeys.OffsetsKey))
    public static final Short CurrentVersion = 0;
    public static final String DefaultClientId = "";

    public static final String SmallestTimeString = "smallest";
    public static final String LargestTimeString = "largest";
    public static final Long LatestTime = -1L;
    public static final Long EarliestTime = -2L;
    public Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo;
    public Short versionId = OffsetRequest.CurrentVersion;
    public Integer correlationId = 0;
    public String clientId = OffsetRequest.DefaultClientId;
    public Integer replicaId = Request.OrdinaryConsumerId;
    // TODO: 2017/10/30 lazy
    private Map<String, Map<TopicAndPartition, PartitionOffsetRequestInfo>> requestInfoGroupedByTopic = Sc.groupByKey(requestInfo, k -> k.topic);

    public OffsetRequest(Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo, Short versionId, Integer correlationId, String clientId, Integer replicaId) {
        super(Optional.of(RequestKeys.OffsetsKey));
        this.requestInfo = requestInfo;
        this.versionId = versionId;
        this.correlationId = correlationId;
        this.clientId = clientId;
        this.replicaId = replicaId;
    }
    public OffsetRequest(Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo, String clientId, Integer replicaId) {
        this(requestInfo, OffsetRequest.CurrentVersion, 0, clientId, replicaId);
    }
    public OffsetRequest(Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo, Integer correlationId, Integer replicaId) {
        this(requestInfo, OffsetRequest.CurrentVersion, correlationId, OffsetRequest.DefaultClientId, replicaId);
    }

    public OffsetRequest readFrom(ByteBuffer buffer) {
        short versionId = buffer.getShort();
        int correlationId = buffer.getInt();
        String clientId = readShortString(buffer);
        int replicaId = buffer.getInt();
        int topicCount = buffer.getInt();
        List<Tuple<TopicAndPartition, PartitionOffsetRequestInfo>> pairs = Sc.itFlatToList(1, topicCount, n -> {
            String topic = readShortString(buffer);
            int partitionCount = buffer.getInt();
            return Sc.itToList(1, partitionCount, m -> {
                int partitionId = buffer.getInt();
                long time = buffer.getLong();
                int maxNumOffsets = buffer.getInt();
                return Tuple.of(new TopicAndPartition(topic, partitionId), new PartitionOffsetRequestInfo(time, maxNumOffsets));
            }).stream();
        });
        return new OffsetRequest(Sc.toMap(pairs), versionId, correlationId, clientId, replicaId);
    }


    public void writeTo(ByteBuffer buffer) {
        buffer.putShort(versionId);
        buffer.putInt(correlationId);
        writeShortString(buffer, clientId);
        buffer.putInt(replicaId);

        buffer.putInt(requestInfoGroupedByTopic.size()); // topic count;
        requestInfoGroupedByTopic.forEach((topic, partitionInfos) -> {
            writeShortString(buffer, topic);
            buffer.putInt(partitionInfos.size()); // partition count;
            partitionInfos.forEach((topicAndPartition, partitionInfo) -> {
                buffer.putInt(topicAndPartition.partition);
                buffer.putLong(partitionInfo.time);
                buffer.putInt(partitionInfo.maxNumOffsets);
            });
        });
    }

    public Integer sizeInBytes() {
        IntCount size = IntCount.of(
                2 + /* versionId */
                        4 + /* correlationId */
                        shortStringLength(clientId) +
                        4 + /* replicaId */
                        4  /* topic count */);
        requestInfoGroupedByTopic.forEach((topic, partitionInfos) ->
                size.add(shortStringLength(topic) +
                        4 + /* partition count */
                        partitionInfos.size() * (
                                4 + /* partition */
                                        8 + /* time */
                                        4 /* maxNumOffsets */)));
        return size.get();
    }

    public boolean isFromOrdinaryClient() {
        return replicaId == Request.OrdinaryConsumerId;
    }

    public boolean isFromDebuggingClient() {
        return replicaId == Request.DebuggingConsumerId;
    }

    @Override
    public String toString() {
        return describe(true);
    }

    @Override
    public void handleError(Throwable e, RequestChannel requestChannel, RequestChannel.Request request) {
        Map<TopicAndPartition, PartitionOffsetsResponse> partitionOffsetResponseMap = Sc.toMap(Sc.map(requestInfo, (topicAndPartition, partitionOffsetRequest) ->
                Tuple.of(topicAndPartition, new PartitionOffsetsResponse(ErrorMapping.codeFor(e.getClass()), null))));
        OffsetResponse errorResponse = new OffsetResponse(correlationId, partitionOffsetResponseMap);
        requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(errorResponse)));
    }

    @Override
    public String describe(Boolean details) {
        StringBuilder offsetRequest = new StringBuilder();
        offsetRequest.append("Name: " + this.getClass().getSimpleName());
        offsetRequest.append("; Version: " + versionId);
        offsetRequest.append("; CorrelationId: " + correlationId);
        offsetRequest.append("; ClientId: " + clientId);
        offsetRequest.append("; ReplicaId: " + replicaId);
        if (details)
            offsetRequest.append("; RequestInfo: " + requestInfo);
        return offsetRequest.toString();
    }
}
