package kafka.api;

import com.google.common.collect.Maps;
import kafka.common.ErrorMapping;
import kafka.func.IntCount;
import kafka.log.TopicAndPartition;

import java.nio.ByteBuffer;
import java.util.Map;

import static kafka.api.ApiUtils.*;

/**
 * @author zhoulf
 * @create 2017-10-12 46 17
 **/

public class StopReplicaResponse extends RequestOrResponse {
    public Integer correlationId;
    public Map<TopicAndPartition, Short> responseMap;
    public Short errorCode = ErrorMapping.NoError;

    public StopReplicaResponse(Integer correlationId, Map<TopicAndPartition, Short> responseMap, Short errorCode) {
        this.correlationId = correlationId;
        this.responseMap = responseMap;
        this.errorCode = errorCode;
    }

    public static StopReplicaResponse readFrom(ByteBuffer buffer) {
        int correlationId = buffer.getInt();
        short errorCode = buffer.getShort();
        int numEntries = buffer.getInt();
        Map<TopicAndPartition, Short> responseMap = Maps.newHashMap();
        for (int i = 0; i < numEntries; i++) {
            String topic = readShortString(buffer);
            Integer partition = buffer.getInt();
            Short partitionErrorCode = buffer.getShort();
            responseMap.put(new TopicAndPartition(topic, partition), partitionErrorCode);
        }
        return new StopReplicaResponse(correlationId, responseMap, errorCode);
    }


    public StopReplicaResponse(Integer correlationId, Map<TopicAndPartition, Short> responseMap) {
        this.correlationId = correlationId;
        this.responseMap = responseMap;
        this.errorCode = ErrorMapping.NoError;
    }

    public Integer sizeInBytes() {
        IntCount size = IntCount.of(0);
        size.add(
                4 /* correlation id */ +
                        2 /* error code */ +
                        4 /* number of responses */);

        for (TopicAndPartition key : responseMap.keySet()) {
            size.add(
                    2 + key.topic.length() /* topic */ +
                            4 /* partition */ +
                            2 /* error code for this partition */);
        }
        return size.get();
    }

    public void writeTo(ByteBuffer buffer) {
        buffer.putInt(correlationId);
        buffer.putShort(errorCode);
        buffer.putInt(responseMap.size());
        for (TopicAndPartition topicAndPartition : responseMap.keySet()) {
            writeShortString(buffer, topicAndPartition.topic);
            buffer.putInt(topicAndPartition.partition);
            buffer.putShort(errorCode);
        }
    }

    @Override
    public String describe(Boolean details) {
        return toString();
    }
}
