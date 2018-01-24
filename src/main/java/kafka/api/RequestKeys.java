package kafka.api;

import kafka.common.KafkaException;
import kafka.func.Handler;
import kafka.func.Tuple;

import java.nio.Buffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2017/4/21.
 */
public class RequestKeys {
    public static final short ProduceKey = 0;
    public static final short FetchKey = 1;
    public static final short OffsetsKey = 2;
    public static final short MetadataKey = 3;
    public static final short LeaderAndIsrKey = 4;
    public static final short StopReplicaKey = 5;
    public static final short UpdateMetadataKey = 6;
    public static final short ControlledShutdownKey = 7;
    public static final short OffsetCommitKey = 8;
    public static final short OffsetFetchKey = 9;
    public static final short ConsumerMetadataKey = 10;
    public static final short JoinGroupKey = 11;
    public static final short HeartbeatKey = 12;

    public static Map<Short, Tuple<String, Handler<Buffer, RequestOrResponse>>> keyToNameAndDeserializerMap =
            new HashMap() {
                {
                    put(ProduceKey, Tuple.of("Produce", ProducerRequest.readFrom));
                    put(FetchKey, Tuple.of("Fetch", FetchRequest.readFrom));
                    put(OffsetsKey, Tuple.of("Offsets", OffsetRequest.readFrom));
                    put(MetadataKey, Tuple.of("Metadata", TopicMetadataRequest.readFrom));
                    put(LeaderAndIsrKey, Tuple.of("LeaderAndIsr", LeaderAndIsrRequest.readFrom));
                    put(StopReplicaKey, Tuple.of("StopReplica", StopReplicaRequest.readFrom));
                    put(UpdateMetadataKey, Tuple.of("UpdateMetadata", UpdateMetadataRequest.readFrom));
                    put(ControlledShutdownKey, Tuple.of("ControlledShutdown", ControlledShutdownRequest.readFrom));
                    put(OffsetCommitKey, Tuple.of("OffsetCommit", OffsetCommitRequest.readFrom));
                    put(OffsetFetchKey, Tuple.of("OffsetFetch", OffsetFetchRequest.readFrom));
                    put(ConsumerMetadataKey, Tuple.of("ConsumerMetadata", ConsumerMetadataRequest.readFrom));
//                    put(JoinGroupKey, Tuple.of("JoinGroup", JoinGroupRequestAndHeader.readFrom));
                    put(HeartbeatKey, Tuple.of("Heartbeat", HeartbeatRequestAndHeader.readFrom));
                }
            };

    public static String nameForKey(Short key) {
        Tuple<String, Handler<Buffer, RequestOrResponse>> t = keyToNameAndDeserializerMap.get(key);
        if (t != null) {
            return t.v1;
        }
        throw new KafkaException(String.format("Wrong request type %d", key));
    }

    public static Handler<Buffer, RequestOrResponse> deserializerForKey(Short key) {
        Tuple<String, Handler<Buffer, RequestOrResponse>> t = keyToNameAndDeserializerMap.get(key);
        if (t != null) {
            return t.v2;
        }
        throw new KafkaException(String.format("Wrong request type %d", key));
    }

}
