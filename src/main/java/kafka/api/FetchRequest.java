package kafka.api;

import kafka.common.ErrorMapping;
import kafka.consumer.ConsumerConfig;
import kafka.func.Handler;
import kafka.func.IntCount;
import kafka.func.Tuple;
import kafka.log.TopicAndPartition;
import kafka.message.MessageSet;
import kafka.network.RequestChannel;
import kafka.utils.Utils;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.*;

import static kafka.api.ApiUtils.*;

/**
 * @author zhoulf
 * @create 2017-10-11 05 11
 **/

public class FetchRequest extends RequestOrResponse {
    public static final Short CurrentVersion = 0;
    public static final Integer DefaultMaxWait = 0;
    public static final Integer DefaultMinBytes = 0;
    public static final Integer DefaultCorrelationId = 0;
    public Short versionId = FetchRequest.CurrentVersion;
    public Integer correlationId = FetchRequest.DefaultCorrelationId;
    public String clientId = ConsumerConfig.DefaultClientId;
    public Integer replicaId = Request.OrdinaryConsumerId;
    public Integer maxWait = FetchRequest.DefaultMaxWait;
    public Integer minBytes = FetchRequest.DefaultMinBytes;
    public Map<TopicAndPartition, PartitionFetchInfo> requestInfo;
    /**
     * Partitions the request info into a map of maps (one for each topic).
     * lazy??
     */
    public Map<String, Map<TopicAndPartition, PartitionFetchInfo>> requestInfoGroupedByTopic;


    public final static Handler<ByteBuffer, FetchRequest> readFrom = (buffer) -> {
        short versionId = buffer.getShort();
        int correlationId = buffer.getInt();
        String clientId = ApiUtils.readShortString(buffer);
        int replicaId = buffer.getInt();
        int maxWait = buffer.getInt();
        int minBytes = buffer.getInt();
        int topicCount = buffer.getInt();
        List<Tuple<TopicAndPartition, PartitionFetchInfo>> pairs = Stream.iterate(1, m -> m + 1).limit(topicCount).flatMap(n -> {
            String topic = readShortString(buffer);
            int partitionCount = buffer.getInt();
            return Stream.iterate(1, m -> m + 1).limit(partitionCount).map(m -> {
                int partitionId = buffer.getInt();
                long offset = buffer.getLong();
                int fetchSize = buffer.getInt();
                return Tuple.of(new TopicAndPartition(topic, partitionId), new PartitionFetchInfo(offset, fetchSize));
            });
        }).collect(Collectors.toList());
        return new FetchRequest(versionId, correlationId, clientId, replicaId, maxWait, minBytes, Utils.toMap(pairs));
    };

    public FetchRequest(Short versionId,
                        Integer correlationId,
                        String clientId,
                        Integer replicaId,
                        Integer maxWait,
                        Integer minBytes,
                        Map<TopicAndPartition, PartitionFetchInfo> requestInfo) {
        this.versionId = versionId;
        this.correlationId = correlationId;
        this.clientId = clientId;
        this.replicaId = replicaId;
        this.maxWait = maxWait;
        this.minBytes = minBytes;
        this.requestInfo = requestInfo;
        requestInfoGroupedByTopic = Utils.groupByKey(requestInfo, k -> k.topic);
    }

    /**
     * Public constructor for the clients
     */
    public FetchRequest(Integer correlationId, String clientId, Integer maxWait, Integer minBytes, Map<TopicAndPartition, PartitionFetchInfo> requestInfo) {
        this(FetchRequest.CurrentVersion,
                correlationId,
                clientId,
                Request.OrdinaryConsumerId,
                maxWait,
                minBytes,
                requestInfo);
    }


    public void writeTo(ByteBuffer buffer) {
        buffer.putShort(versionId);
        buffer.putInt(correlationId);
        writeShortString(buffer, clientId);
        buffer.putInt(replicaId);
        buffer.putInt(maxWait);
        buffer.putInt(minBytes);
        buffer.putInt(requestInfoGroupedByTopic.size()); // topic count;
        requestInfoGroupedByTopic.entrySet().forEach(entry -> {
            String topic = entry.getKey();
            Map<TopicAndPartition, PartitionFetchInfo> partitionFetchInfos = entry.getValue();
            writeShortString(buffer, topic);
            buffer.putInt(partitionFetchInfos.size()); // partition count;
            partitionFetchInfos.entrySet().forEach(entry2 -> {
                Integer partition = entry2.getKey().partition;
                PartitionFetchInfo partitionFetchInfo = entry2.getValue();
                buffer.putInt(partition);
                buffer.putLong(partitionFetchInfo.offset);
                buffer.putInt(partitionFetchInfo.fetchSize);
            });
        });
    }

    public Integer sizeInBytes() {
        IntCount count = IntCount.of(2 + /* versionId */
                4 + /* correlationId */
                shortStringLength(clientId) +
                4 + /* replicaId */
                4 + /* maxWait */
                4 + /* minBytes */
                4  /* topic count */);
        requestInfoGroupedByTopic.entrySet().forEach(entry -> {
            count.add(shortStringLength(entry.getKey()) +
                    4 + /* partition count */
                    entry.getValue().size() * (
                            4 + /* partition id */
                                    8 + /* offset */
                                    4 /* fetch size */
                    ));
        });
        return count.get();
    }

    public boolean isFromFollower() {
        return Request.isValidBrokerId(replicaId);
    }

    public boolean isFromOrdinaryConsumer() {
        return replicaId == Request.OrdinaryConsumerId;
    }

    public boolean isFromLowLevelConsumer() {
        return replicaId == Request.DebuggingConsumerId;
    }

    public Integer numPartitions() {
        return requestInfo.size();
    }

    @Override
    public String toString() {
        return describe(true);
    }

    @Override
    public void handleError(Throwable e, RequestChannel requestChannel, RequestChannel.Request request) {
        Map<TopicAndPartition, FetchResponsePartitionData> fetchResponsePartitionData = Utils.mapValue(requestInfo, v -> new FetchResponsePartitionData(ErrorMapping.codeFor(e.getClass()), -1L, MessageSet.Empty));
        FetchResponse errorResponse = new FetchResponse(correlationId, fetchResponsePartitionData);
        requestChannel.sendResponse(new RequestChannel.Response(request, new FetchResponseSend(errorResponse)));
    }

    @Override
    public String describe(Boolean details) {
        StringBuilder fetchRequest = new StringBuilder();
        fetchRequest.append("Name: " + this.getClass().getSimpleName());
        fetchRequest.append("; Version: " + versionId);
        fetchRequest.append("; CorrelationId: " + correlationId);
        fetchRequest.append("; ClientId: " + clientId);
        fetchRequest.append("; ReplicaId: " + replicaId);
        fetchRequest.append("; MaxWait: " + maxWait + " ms");
        fetchRequest.append("; MinBytes: " + minBytes + " bytes");
        if (details)
            fetchRequest.append("; RequestInfo: " + requestInfo);
        return fetchRequest.toString();
    }
}
