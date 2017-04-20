package kafka.network;/**
 * Created by zhoulf on 2017/4/20.
 */

import kafka.utils.Logging;
import kafka.utils.Time;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.logging.Logger;

/**
 * @author
 * @create 2017-04-20 18:16
 **/
public class Request {
    public Integer processor;
    public Object requestKey;
    private ByteBuffer buffer;
    public Long startTimeMs;
    public SocketAddress remoteAddress = new InetSocketAddress(0);

    public Request(Integer processor, Object requestKey, ByteBuffer buffer, Long startTimeMs, SocketAddress remoteAddress) {
        this.processor = processor;
        this.requestKey = requestKey;
        this.buffer = buffer;
        this.startTimeMs = startTimeMs;
        this.remoteAddress = remoteAddress;
    }

    public volatile Long requestDequeueTimeMs = -1L;
    public volatile Long apiLocalCompleteTimeMs = -1L;
    public volatile Long responseCompleteTimeMs = -1L;
    public volatile Long responseDequeueTimeMs = -1L;
    public Short requestId = buffer.getShort();
    public RequestOrResponse requestObj = RequestKeys.deserializerForKey(requestId) (buffer);
    buffer=null;
    private Logging requestLogger = Logging.getLogger("kafka.request.logger");

    trace(String.format("Processor %d received request : %s",processor, requestObj));

    public void updateRequestMetrics() {
        Long endTimeMs = Time.get().milliseconds();
        // In some corner cases, apiLocalCompleteTimeMs may not be set when the request completes since the remote;
        // processing time is really small. In this case, use responseCompleteTimeMs as apiLocalCompleteTimeMs.;
        if (apiLocalCompleteTimeMs < 0)
            apiLocalCompleteTimeMs = responseCompleteTimeMs;
        Long requestQueueTime = (requestDequeueTimeMs - startTimeMs).max(0L);
        Long apiLocalTime = (apiLocalCompleteTimeMs - requestDequeueTimeMs).max(0L);
        Long apiRemoteTime = (responseCompleteTimeMs - apiLocalCompleteTimeMs).max(0L);
        Long responseQueueTime = (responseDequeueTimeMs - responseCompleteTimeMs).max(0L);
        Long responseSendTime = (endTimeMs - responseDequeueTimeMs).max(0L);
        Long totalTime = endTimeMs - startTimeMs;
        var metricsList = List(RequestMetrics.metricsMap(RequestKeys.nameForKey(requestId)));
        if (requestId == RequestKeys.FetchKey) {
            val isFromFollower = requestObj.asInstanceOf < FetchRequest >.isFromFollower;
            metricsList:: = ( if (isFromFollower)
                RequestMetrics.metricsMap(RequestMetrics.followFetchMetricName);
            else ;
            RequestMetrics.metricsMap(RequestMetrics.consumerFetchMetricName));
        }
        metricsList.foreach {
            m =>m.requestRate.mark();
            m.requestQueueTimeHist.update(requestQueueTime);
            m.localTimeHist.update(apiLocalTime);
            m.remoteTimeHist.update(apiRemoteTime);
            m.responseQueueTimeHist.update(responseQueueTime);
            m.responseSendTimeHist.update(responseSendTime);
            m.totalTimeHist.update(totalTime);
        }
            requestLogger.trace(String.format("Completed request:%s from client %s;totalTime:%d,requestQueueTime:%d,localTime:%d,remoteTime:%d,responseQueueTime:%d,sendTime:%d",
                    requestObj.describe(true), remoteAddress, totalTime, requestQueueTime, apiLocalTime, apiRemoteTime, responseQueueTime, responseSendTime));
            requestLogger.debug(String.format("Completed request:%s from client %s;totalTime:%d,requestQueueTime:%d,localTime:%d,remoteTime:%d,responseQueueTime:%d,sendTime:%d",
            requestObj.describe(false), remoteAddress, totalTime, requestQueueTime, apiLocalTime, apiRemoteTime, responseQueueTime, responseSendTime));
        }
}

