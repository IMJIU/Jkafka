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
public class SyncProducerConfig extends SyncProducerConfigShared {
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
    }

    /**
     * the broker to which the producer sends events
     */
    public String host = props.getString("host");

    /**
     * the port on which the broker is running
     */
    public Integer port = props.getInt("port");

}

abstract class SyncProducerConfigShared {
    VerifiableProperties props;

    Integer sendBufferBytes = props.getInt("send.buffer.bytes", 100 * 1024);

    /* the client application sending the producer requests */
    String clientId = props.getString("client.id", SyncProducerConfig.DefaultClientId);

  /*
   * The number of acknowledgments the producer requires the leader to have received before considering a request complete.
   * This controls the durability of the messages sent by the producer.
   *
   * request.required.acks = 0 - means the producer will not wait for any acknowledgement from the leader.
   * request.required.acks = 1 - means the leader will write the message to its local log and immediately acknowledge
   * request.required.acks = -1 - means the leader will wait for acknowledgement from all in-sync replicas before acknowledging the write
   */

    Short requestRequiredAcks = props.getShortInRange("request.required.acks", SyncProducerConfig.DefaultRequiredAcks, Tuple.of((short) -1, (short) 1));

    /*
     * The ack timeout of the producer requests. Value must be non-negative and non-zero
     */
    Integer requestTimeoutMs = props.getIntInRange("request.timeout.ms", SyncProducerConfig.DefaultAckTimeoutMs, Tuple.of(1, Integer.MAX_VALUE));
}