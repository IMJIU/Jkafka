package kafka.controller.channel;

import kafka.api.*;
import kafka.cluster.Broker;
import kafka.controller.KafkaController;
import kafka.controller.ctrl.ControllerContext;
import kafka.func.ActionP;
import kafka.func.Tuple;
import kafka.network.BlockingChannel;
import kafka.network.Receive;
import kafka.server.StateChangeLogger;
import kafka.utils.ShutdownableThread;
import kafka.utils.Utils;

import java.util.concurrent.BlockingQueue;

/**
 * @author zhoulf
 * @create 2017-11-27 14:27
 **/
public
class RequestSendThread extends ShutdownableThread {
    //(String.format("Controller-%d-to-broker-%d-send-thread",controllerId, toBroker.id))
    public Integer controllerId;
    public ControllerContext controllerContext;
    public Broker toBroker;
    public BlockingQueue<Tuple<RequestOrResponse, ActionP<RequestOrResponse>>> queue;
    public BlockingChannel channel;

    public RequestSendThread(Integer controllerId, ControllerContext controllerContext, Broker toBroker, BlockingQueue<Tuple<RequestOrResponse, ActionP<RequestOrResponse>>> queue, BlockingChannel channel) {
        super(String.format("Controller-%d-to-broker-%d-send-thread", controllerId, toBroker.id));
        this.controllerId = controllerId;
        this.controllerContext = controllerContext;
        this.toBroker = toBroker;
        this.queue = queue;
        this.channel = channel;
        connectToBroker(toBroker, channel);
        stateChangeLogger = KafkaController.stateChangeLogger;
    }

    private Object lock = new Object();
    private StateChangeLogger stateChangeLogger;

    @Override
    public void doWork() throws InterruptedException {
        Tuple<RequestOrResponse, ActionP<RequestOrResponse>> queueItem = queue.take();
        RequestOrResponse request = queueItem.v1;
        ActionP<RequestOrResponse> callback = queueItem.v2;
        Receive receive = null;
        try {
            synchronized (lock) {
                boolean isSendSuccessful = false;
                while (isRunning.get() && !isSendSuccessful) {
                    // if a broker goes down for a long time, then at some point the controller's zookeeper listener will trigger a;
                    // removeBroker which will invoke shutdown() on this thread. At that point, we will stop retrying.;
                    try {
                        channel.send(request);
                        receive = channel.receive();
                        isSendSuccessful = true;
                    } catch (Throwable e) {// if the send was not successful, reconnect to broker and resend the message;
                        logger.warn(String.format("Controller %d epoch %d fails to send request %s to broker %s. " +
                                        "Reconnecting to broker.", controllerId, controllerContext.epoch,
                                request.toString(), toBroker.toString()), e);
                        channel.disconnect();
                        connectToBroker(toBroker, channel);
                        isSendSuccessful = false;
                        // backoff before retrying the connection and send;
                        Utils.swallow(() -> Thread.sleep(300));
                    }
                }
                RequestOrResponse response = null;
                if(request.requestId.isPresent()){
                    Short requestId = request.requestId.get();
                    if (requestId == RequestKeys.LeaderAndIsrKey) {
                        response = LeaderAndIsrResponse.readFrom(receive.buffer());
                    } else if (requestId == RequestKeys.StopReplicaKey) {
                        response = StopReplicaResponse.readFrom(receive.buffer());
                    } else if (requestId == RequestKeys.UpdateMetadataKey) {
                        response = UpdateMetadataResponse.readFrom(receive.buffer());
                    }
                }
                stateChangeLogger.trace(String.format("Controller %d epoch %d received response %s for a request sent to broker %s",
                        controllerId, controllerContext.epoch, response.toString(), toBroker.toString()));

                if (callback != null) {
                    callback.invoke(response);
                }
            }
        } catch (Throwable e) {
            logger.error(String.format("Controller %d fails to send a request to broker %s", controllerId, toBroker.toString()), e);
            // If there is any socket error (eg, socket timeout), the channel is no longer usable and needs to be recreated.;
            channel.disconnect();
        }
    }

    private void connectToBroker(Broker broker, BlockingChannel channel) {
        try {
            channel.connect();
            logger.info(String.format("Controller %d connected to %s for sending state change requests", controllerId, broker.toString()));
        } catch (Throwable e) {
            channel.disconnect();
            logger.error(String.format("Controller %d's connection to broker %s was unsuccessful", controllerId, broker.toString()), e);
        }
    }
}