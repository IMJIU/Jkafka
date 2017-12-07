package kafka.api;

import kafka.common.ErrorMapping;
import kafka.func.Handler;
import kafka.network.BoundedByteBufferSend;
import kafka.network.RequestChannel;

import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * @author zhoulf
 * @create 2017-10-30 26 10
 **/

public class ConsumerMetadataRequest extends RequestOrResponse {
    public static final short CurrentVersion = 0;
    public static final String DefaultClientId = "";
    public String group;
    public Short versionId = ConsumerMetadataRequest.CurrentVersion;
    public Integer correlationId = 0;
    public String clientId = ConsumerMetadataRequest.DefaultClientId;

    public ConsumerMetadataRequest(String group) {
        super(Optional.of(RequestKeys.ConsumerMetadataKey));
        this.group = group;
        this.versionId = ConsumerMetadataRequest.CurrentVersion;
        this.correlationId = 0;
        this.clientId = ConsumerMetadataRequest.DefaultClientId;
    }
    public ConsumerMetadataRequest(String group, Short versionId, Integer correlationId, String clientId) {
        super(Optional.of(RequestKeys.ConsumerMetadataKey));
        this.group = group;
        this.versionId = versionId;
        this.correlationId = correlationId;
        this.clientId = clientId;
    }

    public final static Handler<ByteBuffer, ConsumerMetadataRequest> readFrom = (buffer) -> {
        // envelope;
        short versionId = buffer.getShort();
        int correlationId = buffer.getInt();
        String clientId = ApiUtils.readShortString(buffer);

        // request;
        String group = ApiUtils.readShortString(buffer);
        return new ConsumerMetadataRequest(group, versionId, correlationId, clientId);
    };


    public Integer sizeInBytes() {
        return 2 + /* versionId */
                4 + /* correlationId */
                ApiUtils.shortStringLength(clientId) +
                ApiUtils.shortStringLength(group);
    }


    public void writeTo(ByteBuffer buffer) {
        // envelope;
        buffer.putShort(versionId);
        buffer.putInt(correlationId);
        ApiUtils.writeShortString(buffer, clientId);

        // consumer metadata request;
        ApiUtils.writeShortString(buffer, group);
    }

    @Override
    public   void handleError(Throwable e, RequestChannel requestChannel, RequestChannel.Request request) {
        // return ConsumerCoordinatorNotAvailable for all uncaught errors;
        ConsumerMetadataResponse errorResponse = new ConsumerMetadataResponse(Optional.empty(), ErrorMapping.ConsumerCoordinatorNotAvailableCode);
        requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(errorResponse)));
    }

    public String describe(Boolean details){
        StringBuilder consumerMetadataRequest = new StringBuilder();
        consumerMetadataRequest.append("Name: " + this.getClass().getSimpleName());
        consumerMetadataRequest.append("; Version: " + versionId);
        consumerMetadataRequest.append("; CorrelationId: " + correlationId);
        consumerMetadataRequest.append("; ClientId: " + clientId);
        consumerMetadataRequest.append("; Group: " + group);
        return consumerMetadataRequest.toString();
    }
}
