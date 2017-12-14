package kafka.common;/**
 * Created by zhoulf on 2017/4/25.
 */

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * A bi-directional mapping between error codes and exceptions
 */
public class ErrorMapping {
    public static final ByteBuffer EmptyByteBuffer = ByteBuffer.allocate(0);

    public static final Short UnknownCode = -1;
    public static final Short NoError = 0;
    public static final Short OffsetOutOfRangeCode = 1;
    public static final Short InvalidMessageCode = 2;
    public static final Short UnknownTopicOrPartitionCode = 3;
    public static final Short InvalidFetchSizeCode = 4;
    public static final Short LeaderNotAvailableCode = 5;
    public static final Short NotLeaderForPartitionCode = 6;
    public static final Short RequestTimedOutCode = 7;
    public static final Short BrokerNotAvailableCode = 8;
    public static final Short ReplicaNotAvailableCode = 9;
    public static final Short MessageSizeTooLargeCode = 10;
    public static final Short StaleControllerEpochCode = 11;
    public static final Short OffsetMetadataTooLargeCode = 12;
    public static final Short StaleLeaderEpochCode = 13;
    public static final Short OffsetsLoadInProgressCode = 14;
    public static final Short ConsumerCoordinatorNotAvailableCode = 15;
    public static final Short NotCoordinatorForConsumerCode = 16;
    public static final Short InidTopicCode = 17;
    public static final Short MessageSetSizeTooLargeCode = 18;
    public static final Short NotEnoughReplicasCode = 19;
    public static final Short NotEnoughReplicasAfterAppendCode = 20;

    private static Map<Class<Exception>, Short> exceptionToCode = new HashMap() {{
        put(OffsetOutOfRangeException.class, OffsetOutOfRangeCode);
//        put(InvalidMessageException.class,InvalidMessageCode);
//        put(UnknownTopicOrPartitionException.class,UnknownTopicOrPartitionCode);
        put(InvalidMessageSizeException.class, InvalidFetchSizeCode);
//        put(NotLeaderForPartitionException.class,NotLeaderForPartitionCode);
//        put(LeaderNotAvailableException.class,LeaderNotAvailableCode);
//        put(RequestTimedOutException.class,RequestTimedOutCode);
//        put(BrokerNotAvailableException.class,BrokerNotAvailableCode);
//        put(ReplicaNotAvailableException.class,ReplicaNotAvailableCode);
        put(MessageSizeTooLargeException.class, MessageSizeTooLargeCode);
//        put(ControllerMovedException.class,StaleControllerEpochCode);
//        put(OffsetMetadataTooLargeException.class,OffsetMetadataTooLargeCode);
//        put(OffsetsLoadInProgressException.class,OffsetsLoadInProgressCode);
//        put(ConsumerCoordinatorNotAvailableException.class,ConsumerCoordinatorNotAvailableCode);
//        put(NotCoordinatorForConsumerException.class,NotCoordinatorForConsumerCode);
//        put(InvalidTopicException.class,InvalidTopicCode);
        put(MessageSetSizeTooLargeException.class, MessageSetSizeTooLargeCode);
//        put(NotEnoughReplicasException.class,NotEnoughReplicasCode);
//        put(NotEnoughReplicasAfterAppendException.class,NotEnoughReplicasAfterAppendCode);
    }};

    /* invert the mapping */
    private static final Map<Short, Class<Exception>> codeToException = exceptionToCode.entrySet().stream()
            .collect(Collectors.toMap(kv -> kv.getValue(), kv -> kv.getKey()));

    public static Short codeFor(Class exception) {
        return exceptionToCode.get(exception);
    }

    public void maybeThrowException(Short code) {
        if (code != 0)
            try {
                throw codeToException.get(code).newInstance();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
    }

    public static RuntimeException exceptionFor(Short code) {
        try {
            Class<Exception> c = codeToException.get(code);
            if (c == null) {
                return UnknownCodecException.class.newInstance();
            }
            return new RuntimeException(c.newInstance());
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
