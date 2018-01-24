package kafka.api;

import org.apache.kafka.common.requests.AbstractRequestResponse;

import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * @author zhoulf
 * @create 2018-01-24 00 14
 **/
public abstract class GenericResponseAndHeader extends RequestOrResponse {
    public Integer correlationId;
    public AbstractRequestResponse body;
    public String name;
    public Optional<Short> requestId = Optional.empty();

    public GenericResponseAndHeader(Integer correlationId, AbstractRequestResponse body, String name, Optional<Short> requestId) {
        super(requestId);
        this.correlationId = correlationId;
        this.body = body;
        this.name = name;
        this.requestId = requestId;
    }

    public void writeTo(ByteBuffer buffer) {
        buffer.putInt(correlationId);
        body.writeTo(buffer);
    }

    public Integer sizeInBytes() {
        return 4 /* correlation id */ +
                body.sizeOf();
    }

    @Override
    public String toString() {
        return describe(true);
    }

    @Override
    public String describe(Boolean details) {
        StringBuilder strBuffer = new StringBuilder();
        strBuffer.append("Name: " + name);
        strBuffer.append("; CorrelationId: " + correlationId);
        strBuffer.append("; Body: " + body.toString());
        return strBuffer.toString();
    }
}
