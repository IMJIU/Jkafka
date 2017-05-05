package kafka.network;/**
 * Created by zhoulf on 2017/4/20.
 */

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Metric;
import kafka.api.ProducerRequest;
import kafka.api.RequestKeys;
import kafka.api.RequestOrResponse;
import kafka.func.ActionWithParam;
import kafka.func.Handler;
import kafka.metrics.KafkaMetricsGroup;
import kafka.utils.Logging;
import kafka.utils.Time;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


public class RequestChannel extends KafkaMetricsGroup {
    Integer numProcessors;
    Integer queueSize;
    private List<ActionWithParam<Integer>> responseListeners = Lists.newArrayList();
    private ArrayBlockingQueue<Request> requestQueue ;
    private BlockingQueue<Response>[] responseQueues;
    public Request AllDone;

    public RequestChannel(Integer numProcessors, Integer queueSize) {
        this.numProcessors = numProcessors;
        this.queueSize = queueSize;
        init();
    }

    public void init() {
        requestQueue = new ArrayBlockingQueue<>(queueSize);
        responseQueues = new BlockingQueue[numProcessors];
        AllDone = new Request(1, 2, getShutdownReceive(), 0L);
        for (int i = 0; i < numProcessors; i++)
            responseQueues[i] = new LinkedBlockingQueue<>();

        newGauge("RequestQueueSize",
                new Gauge<Integer>() {
                    @Override
                    public Integer value() {
                        return requestQueue.size();
                    }
                });
        newGauge("ResponseQueueSize",
                new Gauge<Integer>() {
                    @Override
                    public Integer value() {
                        int total = 0;
                        for (BlockingQueue<Response> queue : responseQueues) {
                            total += queue.size();
                        }
                        return total;
                    }
                });
    }


    public ByteBuffer getShutdownReceive() {
        ProducerRequest emptyProducerRequest = new ProducerRequest((short)0, 0, "", (short)0, 0, Maps.newHashMap());
        ByteBuffer byteBuffer = ByteBuffer.allocate(emptyProducerRequest.sizeInBytes() + 2);
        byteBuffer.putShort(RequestKeys.ProduceKey);
        emptyProducerRequest.writeTo(byteBuffer);
        byteBuffer.rewind();
        return byteBuffer;
    }


    /**
     * Send a request to be handled, potentially blocking until there is room in the queue for the request
     */

    public void sendRequest(Request request) throws InterruptedException {
        requestQueue.put(request);
    }

    /**
     * Send a response back to the socket server to be sent over the network
     */
    public void sendResponse(Response response) throws InterruptedException {
        responseQueues[response.processor].put(response);
        for (ActionWithParam onResponse : responseListeners)
            onResponse.invoke(response.processor);
    }

    /**
     * No operation to take for the request, need to read more over the network
     */
    public void noOperation(Integer processor, RequestChannel.Request request) throws InterruptedException {
        responseQueues[processor].put(new Response(processor, request, null, ResponseAction.NoOpAction));
        for (ActionWithParam onResponse : responseListeners)
            onResponse.invoke(processor);
    }

    /**
     * Close the connection for the request
     */
    public void closeConnection(Integer processor, Request request) throws InterruptedException {
        responseQueues[processor].put(new Response(processor, request, null, ResponseAction.CloseConnectionAction));
        for (ActionWithParam onResponse : responseListeners)
            onResponse.invoke(processor);
    }

    /**
     * Get the next request or block until specified time has elapsed
     */
    public Request receiveRequest(Long timeout) throws InterruptedException {
        return requestQueue.poll(timeout, TimeUnit.MILLISECONDS);
    }

    /**
     * Get the next request or block until there is one
     */
    public Request receiveRequest() throws InterruptedException {
        return requestQueue.take();
    }

    /**
     * Get a response for the given processor if there is one
     */
    public Response receiveResponse(Integer processor) {
        Response response = responseQueues[processor].poll();
        if (response != null)
            response.request.responseDequeueTimeMs = Time.get().milliseconds();
        return response;
    }

    public void addResponseListener(ActionWithParam<Integer> onResponse) {
        responseListeners.add(onResponse);
    }

    public void shutdown() {
        requestQueue.clear();
    }

    public static class Request {
        public Integer processor;
        public Object requestKey;
        private ByteBuffer buffer;
        public Long startTimeMs;
        public SocketAddress remoteAddress = new InetSocketAddress(0);

        public Request(Integer processor, Object requestKey, ByteBuffer buffer, Long startTimeMs) {
            this(processor, requestKey, buffer, startTimeMs, new InetSocketAddress(0));
        }

        public Request(Integer processor, Object requestKey, ByteBuffer buffer, Long startTimeMs, SocketAddress
                remoteAddress) {
            this.processor = processor;
            this.requestKey = requestKey;
            this.buffer = buffer;
            this.startTimeMs = startTimeMs;
            this.remoteAddress = remoteAddress;
            init();
        }

        public volatile Long requestDequeueTimeMs = -1L;
        public volatile Long apiLocalCompleteTimeMs = -1L;
        public volatile Long responseCompleteTimeMs = -1L;
        public volatile Long responseDequeueTimeMs = -1L;

        public void init() {
            requestId = buffer.getShort();
            requestObj = RequestKeys.deserializerForKey(requestId).handle(buffer);
            buffer = null;
            requestLogger.trace(String.format("Processor %d received request : %s", processor, requestObj));
        }

        public Short requestId;
        public RequestOrResponse requestObj;
        private Logging requestLogger = Logging.getLogger("kafka.request.logger");


        public void updateRequestMetrics() {
            Long endTimeMs = Time.get().milliseconds();
            // In some corner cases, apiLocalCompleteTimeMs may not be set when the request completes since the remote;
            // processing time is really small. In this case, use responseCompleteTimeMs as apiLocalCompleteTimeMs.;
            if (apiLocalCompleteTimeMs < 0)
                apiLocalCompleteTimeMs = responseCompleteTimeMs;
//            Long requestQueueTime = (requestDequeueTimeMs - startTimeMs).max(0L);
//            Long apiLocalTime = (apiLocalCompleteTimeMs - requestDequeueTimeMs).max(0L);
//            Long apiRemoteTime = (responseCompleteTimeMs - apiLocalCompleteTimeMs).max(0L);
//            Long responseQueueTime = (responseDequeueTimeMs - responseCompleteTimeMs).max(0L);
//            Long responseSendTime = (endTimeMs - responseDequeueTimeMs).max(0L);
            Long totalTime = endTimeMs - startTimeMs;
//            var metricsList = List(RequestMetrics.metricsMap(RequestKeys.nameForKey(requestId)));
//            if (requestId == RequestKeys.FetchKey) {
//                val isFromFollower = requestObj.asInstanceOf < FetchRequest >.isFromFollower;
//                metricsList:: = ( if (isFromFollower)
//                    RequestMetrics.metricsMap(RequestMetrics.followFetchMetricName);
//                else ;
//                RequestMetrics.metricsMap(RequestMetrics.consumerFetchMetricName));
//            }
//            metricsList.foreach {
//                m =>m.requestRate.mark();
//                m.requestQueueTimeHist.update(requestQueueTime);
//                m.localTimeHist.update(apiLocalTime);
//                m.remoteTimeHist.update(apiRemoteTime);
//                m.responseQueueTimeHist.update(responseQueueTime);
//                m.responseSendTimeHist.update(responseSendTime);
//                m.totalTimeHist.update(totalTime);
//            }
//            requestLogger.trace(String.format("Completed request:%s from client %s;totalTime:%d,
// requestQueueTime:%d,localTime:%d,remoteTime:%d,responseQueueTime:%d,sendTime:%d",
//                    requestObj.describe(true), remoteAddress, totalTime, requestQueueTime, apiLocalTime,
// apiRemoteTime, responseQueueTime, responseSendTime));
//            requestLogger.debug(String.format("Completed request:%s from client %s;totalTime:%d,
// requestQueueTime:%d,localTime:%d,remoteTime:%d,responseQueueTime:%d,sendTime:%d",
//                    requestObj.describe(false), remoteAddress, totalTime, requestQueueTime, apiLocalTime,
// apiRemoteTime, responseQueueTime, responseSendTime));
        }
    }

    public static class Response {
        public Integer processor;
        public Request request;
        public Send responseSend;
        public ResponseAction responseAction;

        public Response(Integer processor, Request request, Send responseSend, ResponseAction responseAction) {
            this.processor = processor;
            this.request = request;
            this.responseSend = responseSend;
            this.responseAction = responseAction;
        }

        public Response(Integer processor, Request request, Send responseSend) {
            this(processor, request, responseSend,
                    responseSend == null ? ResponseAction.NoOpAction : ResponseAction.SendAction);
            request.responseCompleteTimeMs = Time.get().milliseconds();
        }


        public Response(Request request, Send send) {
            this(request.processor, request, send);
        }

    }

    enum ResponseAction {
        SendAction, NoOpAction, CloseConnectionAction
    }
}

class RequestMetrics extends KafkaMetricsGroup {
    public String name;
    public static HashMap<String, RequestMetrics> metricsMap = new HashMap<>();
    public static String consumerFetchMetricName = RequestKeys.nameForKey(RequestKeys.FetchKey) + "Consumer";
    public static String followFetchMetricName = RequestKeys.nameForKey(RequestKeys.FetchKey) + "Follower";

    public RequestMetrics(String name) {
        this.name = name;
    }

    static {
        List<String> list = RequestKeys.keyToNameAndDeserializerMap.values().stream().map(e -> e.v1).collect
                (Collectors.toList());
        List<String> nameList = Lists.newArrayList(consumerFetchMetricName, followFetchMetricName);
        list.addAll(nameList);
        list.forEach(name -> metricsMap.put(name, new RequestMetrics(name)));
    }

    public Map<String, String> tags;
    public Metric requestRate;
    // time a request spent in a request queue;
    public Histogram requestQueueTimeHist;
    // time a request takes to be processed at the local broker;
    public Histogram localTimeHist;
    // time a request takes to wait on remote brokers (only relevant to fetch and produce requests);
    public Histogram remoteTimeHist;
    // time a response spent in a response queue;
    public Histogram responseQueueTimeHist;
    // time to send the response to the requester;
    public Histogram responseSendTimeHist;

    public Histogram totalTimeHist;

    public void init() {
        tags = Maps.newHashMap();
        tags.put("request", name);
        requestRate = newMeter("RequestsPerSec", "requests", TimeUnit.SECONDS, tags);
        // time a request spent in a request queue;
        requestQueueTimeHist = newHistogram("RequestQueueTimeMs", true, tags);
        // time a request takes to be processed at the local broker;
        localTimeHist = newHistogram("LocalTimeMs", true, tags);
        // time a request takes to wait on remote brokers (only relevant to fetch and produce requests);
        remoteTimeHist = newHistogram("RemoteTimeMs", true, tags);
        // time a response spent in a response queue;
        responseQueueTimeHist = newHistogram("ResponseQueueTimeMs", true, tags);
        // time to send the response to the requester;
        responseSendTimeHist = newHistogram("ResponseSendTimeMs", true, tags);
        totalTimeHist = newHistogram("TotalTimeMs", true, tags);
    }
}

