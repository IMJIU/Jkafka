package kafka.network;/**
 * Created by zhoulf on 2017/4/20.
 */

import kafka.utils.Time;

/**
 * @author
 * @create 2017-04-20 18:17
 **/
public class Response {
    public Integer processor;
    public Request request;
    public Send responseSend;
    public ResponseAction responseAction;
    public request.responseCompleteTimeMs=Time.get().milliseconds;

    public Response(
            Integer processor, Request
            request,
            Send responseSend
    ) {
        this(processor, request, responseSend, if (responseSend == null) NoOpAction
        else SendAction)
    }


    public Response(Request request, Send send) {
        this(request.processor, request, send);
    }

}

enum ResponseAction {
    SendAction, NoOpAction, CloseConnectionAction
}
