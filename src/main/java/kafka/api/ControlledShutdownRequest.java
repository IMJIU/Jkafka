package kafka.api;

import com.google.common.collect.Sets;
import kafka.common.ErrorMapping;
import kafka.network.BoundedByteBufferSend;
import kafka.network.RequestChannel;

import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * @author zhoulf
 * @create 2017-11-28 49 13
 **/

public class ControlledShutdownRequest extends RequestOrResponse {
    public static short CurrentVersion = 0;
    String DefaultClientId = "";
    short versionId;
    int correlationId;
    int brokerId;

    public ControlledShutdownRequest(short versionId, int correlationId, int brokerId) {
        super(Optional.of(RequestKeys.ControlledShutdownKey));
        this.versionId = versionId;
        this.correlationId = correlationId;
        this.brokerId = brokerId;
    }

    public static ControlledShutdownRequest readFrom(ByteBuffer buffer) {
        short versionId = buffer.getShort();
        int correlationId = buffer.getInt();
        int brokerId = buffer.getInt();
        return new ControlledShutdownRequest(versionId, correlationId, brokerId);
    }

//        extends RequestOrResponse(Some(RequestKeys.ControlledShutdownKey)){

    public ControlledShutdownRequest(Integer correlationId, Integer brokerId) {
        this(ControlledShutdownRequest.CurrentVersion, correlationId, brokerId);
    }

    public void writeTo(ByteBuffer buffer) {
        buffer.putShort(versionId);
        buffer.putInt(correlationId);
        buffer.putInt(brokerId);
    }

    public Integer sizeInBytes() {
        return 2 +  /* version id */
                4 + /* correlation id */
                4 /* broker id */;
    }

    @Override
    public String toString() {
        return describe(true);
    }

    @Override
    public void handleError(Throwable e, RequestChannel requestChannel, RequestChannel.Request request) {
        ControlledShutdownResponse errorResponse = new ControlledShutdownResponse(correlationId, ErrorMapping.codeFor(e.getCause().getClass()),
                Sets.newHashSet());
        requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(errorResponse)));
    }

    @Override
    public String describe(Boolean details) {
        if (details == null) {
            details = false;
        }
        StringBuilder controlledShutdownRequest = new StringBuilder();
        controlledShutdownRequest.append("Name: " + this.getClass().getSimpleName());
        controlledShutdownRequest.append("; Version: " + versionId);
        controlledShutdownRequest.append("; CorrelationId: " + correlationId);
        controlledShutdownRequest.append("; BrokerId: " + brokerId);
        return controlledShutdownRequest.toString();
    }
}
