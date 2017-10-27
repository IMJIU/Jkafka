package kafka.api;

import kafka.cluster.Broker;
import kafka.func.Tuple;
import kafka.utils.Sc;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * @author zhoulf
 * @create 2017-10-27 25 17
 **/

public class TopicMetadataResponse extends RequestOrResponse {
    public List<Broker> brokers;
    public List<TopicMetadata> topicsMetadata;
    public Integer correlationId;



    public TopicMetadataResponse(List<Broker> brokers, List<TopicMetadata> topicsMetadata, Integer correlationId) {
        this.brokers = brokers;
        this.topicsMetadata = topicsMetadata;
        this.correlationId = correlationId;

    }

    public static TopicMetadataResponse readFrom(ByteBuffer buffer) {
        int correlationId = buffer.getInt();
        int brokerCount = buffer.getInt();
        List<Broker> brokers = Sc.itToList(0, brokerCount, n -> Broker.readFrom(buffer));
        Map<Integer, Broker> brokerMap = Sc.toMap(Sc.map(brokers, b -> Tuple.of(b.id, b)));
        int topicCount = buffer.getInt();
        List<TopicMetadata> topicsMetadata = Sc.itToList(0, topicCount, n -> TopicMetadata.readFrom(buffer, brokerMap));
        return new TopicMetadataResponse(brokers, topicsMetadata, correlationId);
    }


    public void writeTo(ByteBuffer buffer) {
        buffer.putInt(correlationId);
    /* brokers */
        buffer.putInt(brokers.size());
        brokers.forEach(b -> b.writeTo(buffer));
    /* topic metadata */
        buffer.putInt(topicsMetadata.size());
        topicsMetadata.forEach(t -> t.writeTo(buffer));
    }

    public Integer sizeInBytes() {
        return 4 + 4 + Sc.sum(brokers, b -> b.sizeInBytes()) + 4
                + Sc.sum(topicsMetadata, t -> t.sizeInBytes());
    }

    @Override
    public String describe(Boolean details) {
        return this.toString();
    }
}
