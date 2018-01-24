package kafka.api;

import kafka.common.ErrorMapping;
import kafka.func.Handler;
import kafka.network.BoundedByteBufferSend;
import kafka.network.RequestChannel;
import org.apache.kafka.common.requests.HeartbeatRequest;
import org.apache.kafka.common.requests.HeartbeatResponse;

import java.nio.ByteBuffer;
import java.util.Optional;

import static kafka.api.ApiUtils.*;

/**
 * @author zhoulf
 * @create 2018-01-24 36 13
 **/

public class HeartbeatRequestAndHeader extends GenericRequestAndHeader {
    public Short versionId;
    public Integer correlationId;
    public String clientId;
    public HeartbeatRequest body;

    public HeartbeatRequestAndHeader(Short versionId, Integer correlationId, String clientId, HeartbeatRequest body) {
        super(versionId, correlationId, clientId, body, RequestKeys.nameForKey(RequestKeys.HeartbeatKey), Optional.of(RequestKeys.HeartbeatKey));
        this.versionId = versionId;
        this.correlationId = correlationId;
        this.clientId = clientId;
        this.body = body;

    }

    public static final Handler<ByteBuffer, HeartbeatRequestAndHeader> readFrom = (ByteBuffer buffer) -> {
        short versionId = buffer.getShort();
        int correlationId = buffer.getInt();
        String clientId = readShortString(buffer);
        HeartbeatRequest body = org.apache.kafka.common.requests.HeartbeatRequest.parse(buffer);
        return new HeartbeatRequestAndHeader(versionId, correlationId, clientId, body);
    };


    @Override
    public void handleError(Throwable e, RequestChannel requestChannel, RequestChannel.Request request) {
        HeartbeatResponse errorResponseBody = new HeartbeatResponse(ErrorMapping.codeFor(e.getClass()));
        HeartbeatResponseAndHeader errorHeartBeatResponseAndHeader = new HeartbeatResponseAndHeader(correlationId, errorResponseBody);
        requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(errorHeartBeatResponseAndHeader)));
    }
}

