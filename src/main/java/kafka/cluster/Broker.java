package kafka.cluster;

import com.alibaba.fastjson.JSON;
import kafka.api.ApiUtils;
import kafka.common.BrokerNotAvailableException;
import kafka.common.KafkaException;
import kafka.utils.Utils;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * A Kafka broker
 */
public class Broker {
    public Integer id;
    public String host;
    public Integer port;

    public Broker(Integer id, String host, Integer port) {
        this.id = id;
        this.host = host;
        this.port = port;
    }

    public static Broker createBroker(Integer id, String brokerInfoString) {
        if (brokerInfoString == null)
            throw new BrokerNotAvailableException(String.format("Broker id %s does not exist", id));
        try {
            Map<String, Object> brokerInfo = JSON.parseObject(brokerInfoString, Map.class);
            if (brokerInfo != null) {
                String host = brokerInfo.get("host").toString();
                Integer port = Integer.parseInt(brokerInfo.get("port").toString());
                return new Broker(id, host, port);
            } else {
                throw new BrokerNotAvailableException(String.format("Broker id %d does not exist", id));
            }
        } catch (Throwable t) {
            throw new KafkaException("Failed to parse the broker info from zookeeper: " + brokerInfoString, t);
        }
    }

    public Broker readFrom(ByteBuffer buffer) {
        Integer id = buffer.getInt();
        String host = ApiUtils.readShortString(buffer);
        Integer port = buffer.getInt();
        return new Broker(id, host, port);
    }


    @Override
    public String toString() {
        return "id:" + id + ",host:" + host + ",port:" + port;
    }

    public String connectionString() {
        return org.apache.kafka.common.utils.Utils.formatAddress(host, port);
    }

    public void writeTo(ByteBuffer buffer) {
        buffer.putInt(id);
        ApiUtils.writeShortString(buffer, host);
        buffer.putInt(port);
    }

    public Integer sizeInBytes() {
        return ApiUtils.shortStringLength(host) /* host name */ + 4 /* port */ + 4; /* broker id*/
    }

    @Override
    public boolean equals(Object obj) {
        if (obj != null && obj instanceof Broker) {
            Broker n = (Broker) obj;
            return id == n.id && host == n.host && port == n.port;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Utils.hashcode(id, host, port);
    }
}
