package kafka.api;

import kafka.common.ErrorMapping;

import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * @author zhoulf
 * @create 2017-10-19 22 14
 **/

public class UpdateMetadataResponse extends RequestOrResponse {
    public Integer correlationId;
    public Short errorCode;

    public UpdateMetadataResponse(Integer correlationId, Short errorCode) {
        this.correlationId = correlationId;
        this.errorCode = errorCode;
    }


    public static UpdateMetadataResponse readFrom(ByteBuffer buffer) {
        int correlationId = buffer.getInt();
        short errorCode = buffer.getShort();
        return new UpdateMetadataResponse(correlationId, errorCode);
    }

    public Integer sizeInBytes() {
        return 4 /* correlation id */ + 2 /* error code */;
    }

    public void writeTo(ByteBuffer buffer) {
        buffer.putInt(correlationId);
        buffer.putShort(errorCode);
    }

    @Override
    public String describe(Boolean details) {
        return this.toString();
    }
}
