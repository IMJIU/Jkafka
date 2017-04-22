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
    public static final Short ProduceKey = 0;
    public static final Short FetchKey = 1;
    public static final Short OffsetsKey = 2;
    public static final Short MetadataKey = 3;
    public static final Short LeaderAndIsrKey = 4;
    public static final Short StopReplicaKey = 5;
    public static final Short UpdateMetadataKey = 6;
    public static final Short ControlledShutdownKey = 7;
    public static final Short OffsetCommitKey = 8;
    public static final Short OffsetFetchKey = 9;
    public static final Short ConsumerMetadataKey = 10;
    public static final Short JoinGroupKey = 11;
    public static final Short HeartbeatKey = 12;

    public static Map<Short, Tuple<String, Handler<Buffer, RequestOrResponse>>> keyToNameAndDeserializerMap =
            new HashMap() {
                {
                    put(ProduceKey, Tuple.of("Produce", ProducerRequest.readFrom));
//                    put(FetchKey, Tuple.of("Fetch", FetchRequest.readFrom));
//                    put(OffsetsKey, Tuple.of("Offsets", OffsetRequest.readFrom));
//                    put(MetadataKey, Tuple.of("Metadata", TopicMetadataRequest.readFrom));
//                    put(LeaderAndIsrKey, Tuple.of("LeaderAndIsr", LeaderAndIsrRequest.readFrom));
//                    put(StopReplicaKey, Tuple.of("StopReplica", StopReplicaRequest.readFrom));
//                    put(UpdateMetadataKey, Tuple.of("ControlledShutdown", ControlledShutdownRequest.readFrom));
//                    put(ControlledShutdownKey, Tuple.of("Produce", ProducerRequest.readFrom));
//                    put(OffsetCommitKey, Tuple.of("OffsetCommit", OffsetCommitRequest.readFrom));
//                    put(OffsetFetchKey, Tuple.of("OffsetFetch", OffsetFetchRequest.readFrom));
//                    put(ConsumerMetadataKey, Tuple.of("ConsumerMetadata", ConsumerMetadataRequest.readFrom));
//                    put(JoinGroupKey, Tuple.of("ConsumerMetadata", ConsumerMetadataRequest.readFrom));
//                    put(HeartbeatKey, Tuple.of("Heartbeat", HeartbeatRequestAndHeader.readFrom));
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
