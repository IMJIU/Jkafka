package kafka.api;

import kafka.network.RequestChannel;
import kafka.utils.Logging;

import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * Created by Administrator on 2017/4/21.
 */
public abstract class RequestOrResponse extends Logging {
    public Optional<Short> requestId = Optional.empty();
    public RequestOrResponse() {
        this.requestId = Optional.empty();
    }
    public RequestOrResponse(Optional<Short> requestId) {
        this.requestId = requestId;
    }

    public abstract Integer sizeInBytes();

    public abstract void writeTo(ByteBuffer buffer);

    public void handleError(Throwable e, RequestChannel requestChannel, RequestChannel.Request request) {
    }

    /* The purpose of this API is to return a string description of the Request mainly for the purpose of request logging.
    *  This API has no meaning for a Response object.
     * @param details If this is false, omit the parts of the request description that are proportional to the number of
     *                topics or partitions. This is mainly to control the amount of request logging. */
    public abstract String describe(Boolean details);

}

