package kafka.controller.channel;

import kafka.api.RequestOrResponse;
import kafka.cluster.Broker;
import kafka.func.ActionP;
import kafka.func.Tuple;
import kafka.network.BlockingChannel;

import java.util.concurrent.BlockingQueue;

/**
 * @author zhoulf
 * @create 2017-11-27 14:26
 **/
public class ControllerBrokerStateInfo {
    public BlockingChannel channel;
    public Broker broker;
    public BlockingQueue<Tuple<RequestOrResponse, ActionP<RequestOrResponse>>> messageQueue;
    public RequestSendThread requestSendThread;

    public ControllerBrokerStateInfo(BlockingChannel channel, Broker broker, BlockingQueue<Tuple<RequestOrResponse, ActionP<RequestOrResponse>>> messageQueue, RequestSendThread requestSendThread) {
        this.channel = channel;
        this.broker = broker;
        this.messageQueue = messageQueue;
        this.requestSendThread = requestSendThread;
    }
}