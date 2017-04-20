package kafka.network;/**
 * Created by zhoulf on 2017/4/20.
 */

import com.google.common.collect.Lists;
import com.yammer.metrics.core.Gauge;
import kafka.func.Fun;
import kafka.metrics.KafkaMetricsGroup;
import kafka.utils.Logging;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author
 * @create 2017-04-20 14 18
 **/
public class RequestChannel extends KafkaMetricsGroup {
    Integer numProcessors;
    Integer queueSize;
    private List<Fun<Integer>> responseListeners = Lists.newArrayList();
    private ArrayBlockingQueue<Request> requestQueue = new ArrayBlockingQueue<>(queueSize);
    private BlockingQueue<Response>[] responseQueues = new BlockingQueue[numProcessors];
    public Request AllDone;

    public RequestChannel(Integer numProcessors, Integer queueSize) {
        this.numProcessors = numProcessors;
        this.queueSize = queueSize;
    }

    public void init() {
        AllDone = new Request(1, 2, getShutdownReceive(), 0);
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


    public void getShutdownReceive() {
        val emptyProducerRequest = new ProducerRequest(0, 0, "", 0, 0, collection.mutable.Map < TopicAndPartition, ByteBufferMessageSet > ());
        val byteBuffer = ByteBuffer.allocate(emptyProducerRequest.sizeInBytes + 2);
        byteBuffer.putShort(RequestKeys.ProduceKey);
        emptyProducerRequest.writeTo(byteBuffer);
        byteBuffer.rewind();
        byteBuffer;
    }





        for(i< -0
    until numProcessors)

    {
        newGauge("ResponseQueueSize",
                new Gauge<Integer> {
        public void value = responseQueues(i).size();
    },
        Map("processor" ->i.toString);
    );
    }

    /**
     * Send a request to be handled, potentially blocking until there is room in the queue for the request
     */

    public void sendRequest(RequestChannel request.Request) {
        requestQueue.put(request);
    }

    /**
     * Send a response back to the socket server to be sent over the network
     */
    public void sendResponse(RequestChannel response.Response) {
        responseQueues(response.processor).put(response);
        for (onResponse< -responseListeners)
            onResponse(response.processor);
    }

    /**
     * No operation to take for the request, need to read more over the network
     */
    public void noOperation(Integer processor, RequestChannel request.Request) {
        responseQueues(processor).put(new RequestChannel.Response(processor, request, null, RequestChannel.NoOpAction));
        for (onResponse< -responseListeners)
            onResponse(processor);
    }

    /**
     * Close the connection for the request
     */
    public void closeConnection(Integer processor, RequestChannel request.Request) {
        responseQueues(processor).put(new RequestChannel.Response(processor, request, null, RequestChannel.CloseConnectionAction));
        for (onResponse< -responseListeners)
            onResponse(processor);
    }

    /**
     * Get the next request or block until specified time has elapsed
     */
    public void receiveRequest(Long timeout):RequestChannel.Request =
            requestQueue.poll(timeout,TimeUnit.MILLISECONDS);

    /**
     * Get the next request or block until there is one
     */
    public void receiveRequest():RequestChannel.Request =
            requestQueue.take();

    /**
     * Get a response for the given processor if there is one
     */
    public void receiveResponse(Integer processor):RequestChannel.Response =

    {
        val response = responseQueues(processor).poll();
        if (response != null)
            response.request.responseDequeueTimeMs = SystemTime.milliseconds;
        response;
    }

    public void addResponseListener(Integer onResponse =>Unit) {
        responseListeners:: = onResponse;
    }

    public void shutdown() {
        requestQueue.clear;
    }
}

    object RequestMetrics {
        val metricsMap=new scala.collection.mutable.HashMap<String, RequestMetrics>
        val consumerFetchMetricName=RequestKeys.nameForKey(RequestKeys.FetchKey)+"Consumer";
                val followFetchMetricName=RequestKeys.nameForKey(RequestKeys.FetchKey)+"Follower";
                (RequestKeys.keyToNameAndDeserializerMap.values.map(e=>e._1);
                ++List(consumerFetchMetricName,followFetchMetricName)).foreach(name=>metricsMap.put(name,new RequestMetrics(name)))
                }

class RequestMetrics(String name)extends KafkaMetricsGroup{
        val tags=Map("request"->name);
        val requestRate=newMeter("RequestsPerSec","requests",TimeUnit.SECONDS,tags);
        // time a request spent in a request queue;
        val requestQueueTimeHist=newHistogram("RequestQueueTimeMs",biased=true,tags);
        // time a request takes to be processed at the local broker;
        val localTimeHist=newHistogram("LocalTimeMs",biased=true,tags);
        // time a request takes to wait on remote brokers (only relevant to fetch and produce requests);
        val remoteTimeHist=newHistogram("RemoteTimeMs",biased=true,tags);
        // time a response spent in a response queue;
        val responseQueueTimeHist=newHistogram("ResponseQueueTimeMs",biased=true,tags);
        // time to send the response to the requester;
        val responseSendTimeHist=newHistogram("ResponseSendTimeMs",biased=true,tags);
        val totalTimeHist=newHistogram("TotalTimeMs",biased=true,tags);
        }

        }
