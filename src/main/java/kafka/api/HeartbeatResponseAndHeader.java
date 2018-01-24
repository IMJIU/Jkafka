package kafka.api;

import org.apache.kafka.common.requests.HeartbeatResponse;

import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * @author zhoulf
 * @create 2018-01-24 57 13
 **/
public class HeartbeatResponseAndHeader extends GenericResponseAndHeader {
    public Integer correlationId;
    public HeartbeatResponse body;

    public HeartbeatResponseAndHeader(Integer correlationId, HeartbeatResponse body) {
        super(correlationId, body, RequestKeys.nameForKey(RequestKeys.HeartbeatKey), Optional.empty());
        this.correlationId = correlationId;
        this.body = body;
    }

    public HeartbeatResponseAndHeader readFrom(ByteBuffer buffer) {
        int correlationId = buffer.getInt();
        HeartbeatResponse body = HeartbeatResponse.parse(buffer);
        return new HeartbeatResponseAndHeader(correlationId, body);
    }
}

