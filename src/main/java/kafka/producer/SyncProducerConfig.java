package kafka.producer;/**
 * Created by zhoulf on 2017/5/3.
 */

import kafka.func.Tuple;
import kafka.utils.VerifiableProperties;

import java.util.Properties;

/**
 * @author
 * @create 2017-05-03 09 14
 **/
public class SyncProducerConfig implements SyncProducerConfigShared {
    public VerifiableProperties props;
    public static String DefaultClientId = "";
    public static Short DefaultRequiredAcks = 0;
    public static Integer DefaultAckTimeoutMs = 10000;

    public SyncProducerConfig(VerifiableProperties props) {
        this.props = props;
    }

    public SyncProducerConfig(Properties originalProps) {
        this(new VerifiableProperties(originalProps));
        // no need to verify the property since SyncProducerConfig is supposed to be used internally;
        host = props.getString("host");
        port = props.getInt("port");
    }

    /**
     * the broker to which the producer sends events
     */
    public String host ;

    /**
     * the port on which the broker is running
     */
    public Integer port ;

    @Override
    public VerifiableProperties props() {
        return props;
    }
}

interface SyncProducerConfigShared {
    VerifiableProperties props();

    default Integer sendBufferBytes() {
        return props().getInt("send.buffer.bytes", 100 * 1024);
    }

    /* the client application sending the producer requests */
    default String clientId() {
        return props().getString("client.id", SyncProducerConfig.DefaultClientId);
    }

  /*
   * The number of acknowledgments the producer requires the leader to have received before considering a request complete.
   * This controls the durability of the messages sent by the producer.
   *
   * request.required.acks = 0 - means the producer will not wait for any acknowledgement from the leader.
   * request.required.acks = 1 - means the leader will write the message to its local log and immediately acknowledge
   * request.required.acks = -1 - means the leader will wait for acknowledgement from all in-sync replicas before acknowledging the write
   */

    default Short requestRequiredAcks() {
        return props().getShortInRange("request.required.acks", SyncProducerConfig.DefaultRequiredAcks, Tuple.of((short) -1, (short) 1));
    }

    /*
     * The ack timeout of the producer requests. Value must be non-negative and non-zero
     */
    default Integer requestTimeoutMs() {
        return props().getIntInRange("request.timeout.ms", SyncProducerConfig.DefaultAckTimeoutMs, Tuple.of(1, Integer.MAX_VALUE));
    }
}