package kafka.api;

import com.google.common.collect.Maps;
import kafka.common.ErrorMapping;
import kafka.func.IntCount;
import kafka.func.Tuple;
import kafka.utils.Utils;

import java.nio.ByteBuffer;
import java.util.Map;

import static kafka.api.ApiUtils.*;

/**
 * @author zhoulf
 * @create 2017-10-20 24 11
 **/

public class LeaderAndIsrResponse extends RequestOrResponse {
    public Integer correlationId;
    public Map<Tuple<String, Integer>, Short> responseMap;
    public Short errorCode;

    public LeaderAndIsrResponse(Integer correlationId, Map<Tuple<String, Integer>, Short> responseMap) {
        this(correlationId, responseMap, ErrorMapping.NoError);
    }

    public LeaderAndIsrResponse(Integer correlationId, Map<Tuple<String, Integer>, Short> responseMap, Short errorCode) {
        this.correlationId = correlationId;
        this.responseMap = responseMap;
        this.errorCode = errorCode;
    }

    public static LeaderAndIsrResponse readFrom(ByteBuffer buffer) {
        int correlationId = buffer.getInt();
        short errorCode = buffer.getShort();
        int numEntries = buffer.getInt();
        Map<Tuple<String, Integer>, Short> responseMap = Maps.newHashMap();
        for (int i = 0; i < numEntries; i++) {
            String topic = readShortString(buffer);
            int partition = buffer.getInt();
            short partitionErrorCode = buffer.getShort();
            responseMap.put(Tuple.of(topic, partition), partitionErrorCode);
        }
        return new LeaderAndIsrResponse(correlationId, responseMap, errorCode);
    }


    public Integer sizeInBytes() {
        IntCount size = IntCount.of(
                4 /* correlation id */ +
                        2 /* error code */ +
                        4 /* number of responses */);
        Utils.foreach(responseMap, (key, value) ->
                size.add(
                        2 + key.v1.length() /* topic */ +
                                4 /* partition */ +
                                2 /* error code for this partition */));
        return size.get();
    }

    public void writeTo(ByteBuffer buffer) {
        buffer.putInt(correlationId);
        buffer.putShort(errorCode);
        buffer.putInt(responseMap.size());
        Utils.foreach(responseMap, (key, value) -> {
            writeShortString(buffer, key.v1);
            buffer.putInt(key.v2);
            buffer.putShort(value);
        });
    }

    @Override
    public String describe(Boolean details) {
        return this.toString();
    }

}
