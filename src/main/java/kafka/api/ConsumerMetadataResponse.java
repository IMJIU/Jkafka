package kafka.api;

import kafka.cluster.Broker;
import kafka.common.ErrorMapping;

import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * @author zhoulf
 * @create 2017-10-30 20 10
 **/

public class ConsumerMetadataResponse extends RequestOrResponse {
    public static final Integer CurrentVersion = 0;
    public Optional<Broker> coordinatorOpt;
    public Short errorCode;
    public Integer correlationId = 0;
    private final static Broker NoBroker = new Broker(-1, "", -1);

    public ConsumerMetadataResponse(Optional<Broker> coordinatorOpt, Short errorCode) {
        this(coordinatorOpt, errorCode, 0);
    }

    public ConsumerMetadataResponse(Optional<Broker> coordinatorOpt, Short errorCode, Integer correlationId) {
        this.coordinatorOpt = coordinatorOpt;
        this.errorCode = errorCode;
        this.correlationId = correlationId;
    }

    public static ConsumerMetadataResponse readFrom(ByteBuffer buffer) {
        int correlationId = buffer.getInt();
        short errorCode = buffer.getShort();
        Broker broker = Broker.readFrom(buffer);
        Optional<Broker> coordinatorOpt = (errorCode == ErrorMapping.NoError) ? Optional.of(broker) : Optional.empty();

        return new ConsumerMetadataResponse(coordinatorOpt, errorCode, correlationId);
    }


    public Integer sizeInBytes() {
        return 4 + /* correlationId */
                2 + /* error code */
                coordinatorOpt.orElse(ConsumerMetadataResponse.NoBroker).sizeInBytes();
    }


    public void writeTo(ByteBuffer buffer) {
        buffer.putInt(correlationId);
        buffer.putShort(errorCode);
        coordinatorOpt.orElse(ConsumerMetadataResponse.NoBroker).writeTo(buffer);
    }

    public String describe(Boolean details) {
        return this.toString();
    }
}
