package kafka.network;/**
 * Created by zhoulf on 2017/4/20.
 */

/**
 * @author
 * @create 2017-04-20 18:16
 **/
public class Request {
    class Request(Integer processor, Any requestKey, private var ByteBuffer buffer, Long startTimeMs, SocketAddress remoteAddress = new InetSocketAddress(0)) {
    @volatile var requestDequeueTimeMs = -1L
    @volatile var apiLocalCompleteTimeMs = -1L
    @volatile var responseCompleteTimeMs = -1L
    @volatile var responseDequeueTimeMs = -1L
        val requestId = buffer.getShort();
        val RequestOrResponse requestObj = RequestKeys.deserializerForKey(requestId)(buffer);
        buffer = null;
        private val requestLogger = Logger.getLogger("kafka.request.logger");
        trace(String.format("Processor %d received request : %s",processor, requestObj))

        public void updateRequestMetrics() {
            val endTimeMs = SystemTime.milliseconds;
            // In some corner cases, apiLocalCompleteTimeMs may not be set when the request completes since the remote;
            // processing time is really small. In this case, use responseCompleteTimeMs as apiLocalCompleteTimeMs.;
            if (apiLocalCompleteTimeMs < 0)
                apiLocalCompleteTimeMs = responseCompleteTimeMs;
            val requestQueueTime = (requestDequeueTimeMs - startTimeMs).max(0L);
            val apiLocalTime = (apiLocalCompleteTimeMs - requestDequeueTimeMs).max(0L);
            val apiRemoteTime = (responseCompleteTimeMs - apiLocalCompleteTimeMs).max(0L);
            val responseQueueTime = (responseDequeueTimeMs - responseCompleteTimeMs).max(0L);
            val responseSendTime = (endTimeMs - responseDequeueTimeMs).max(0L);
            val totalTime = endTimeMs - startTimeMs;
            var metricsList = List(RequestMetrics.metricsMap(RequestKeys.nameForKey(requestId)));
            if (requestId == RequestKeys.FetchKey) {
                val isFromFollower = requestObj.asInstanceOf<FetchRequest>.isFromFollower;
                metricsList ::= ( if (isFromFollower)
                    RequestMetrics.metricsMap(RequestMetrics.followFetchMetricName);
                else;
                RequestMetrics.metricsMap(RequestMetrics.consumerFetchMetricName) );
            }
            metricsList.foreach{
                m => m.requestRate.mark();
                m.requestQueueTimeHist.update(requestQueueTime);
                m.localTimeHist.update(apiLocalTime);
                m.remoteTimeHist.update(apiRemoteTime);
                m.responseQueueTimeHist.update(responseQueueTime);
                m.responseSendTimeHist.update(responseSendTime);
                m.totalTimeHist.update(totalTime);
            }
            if(requestLogger.isTraceEnabled)
                requestLogger.trace("Completed request:%s from client %s;totalTime:%d,requestQueueTime:%d,localTime:%d,remoteTime:%d,responseQueueTime:%d,sendTime:%d";
                            .format(requestObj.describe(true), remoteAddress, totalTime, requestQueueTime, apiLocalTime, apiRemoteTime, responseQueueTime, responseSendTime))
                else if(requestLogger.isDebugEnabled) {
                requestLogger.debug("Completed request:%s from client %s;totalTime:%d,requestQueueTime:%d,localTime:%d,remoteTime:%d,responseQueueTime:%d,sendTime:%d";
                            .format(requestObj.describe(false), remoteAddress, totalTime, requestQueueTime, apiLocalTime, apiRemoteTime, responseQueueTime, responseSendTime))
            }
        }
    }

}
