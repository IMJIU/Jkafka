package kafka.api;

import org.apache.kafka.common.requests.AbstractRequestResponse;

import java.nio.ByteBuffer;
import java.util.Optional;

import static kafka.api.ApiUtils.*;

/**
 * @author zhoulf
 * @create 2018-01-24 51 13
 **/
public abstract class GenericRequestAndHeader extends RequestOrResponse {
    public Short versionId;
    public Integer correlationId;
    public String clientId;
    public AbstractRequestResponse body;
    public String name;
    public Optional<Short> requestId = Optional.empty();

    public GenericRequestAndHeader(Short versionId, Integer correlationId, String clientId, AbstractRequestResponse body, String name, Optional<Short> requestId) {
        super(requestId);
        this.versionId = versionId;
        this.correlationId = correlationId;
        this.clientId = clientId;
        this.body = body;
        this.name = name;
        this.requestId = requestId;
    }

    public void writeTo(ByteBuffer buffer) {
        buffer.putShort(versionId);
        buffer.putInt(correlationId);
        writeShortString(buffer, clientId);
        body.writeTo(buffer);
    }

    public Integer sizeInBytes() {
        return 2 /* version id */ +
                4 /* correlation id */ +
                (2 + clientId.length()) /* client id */ +
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
        strBuffer.append("; Version: " + versionId);
        strBuffer.append("; CorrelationId: " + correlationId);
        strBuffer.append("; ClientId: " + clientId);
        strBuffer.append("; Body: " + body.toString());
        return strBuffer.toString();
    }
}
