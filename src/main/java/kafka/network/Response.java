package kafka.network;/**
 * Created by zhoulf on 2017/4/20.
 */

/**
 * @author
 * @create 2017-04-20 18:17
 **/
public class Response {
    Integer processor;
    Request request;
    Send responseSend;
    ResponseAction responseAction;
    request.responseCompleteTimeMs =SystemTime.milliseconds;

    public void this(
    Integer processor, Request
    request,
    Send responseSend)=
            this(processor,request,responseSend,if(responseSend ==null)NoOpAction else SendAction)

    public void this(
    Request request, Send
    send)=
            this(request.processor,request,send);
}

    trait ResponseAction;
        case object SendAction extends ResponseAction;
                case object NoOpAction extends ResponseAction;
                case object CloseConnectionAction extends ResponseAction;
                }

                }
