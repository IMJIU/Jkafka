package kafka.controller;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import kafka.api.*;
import kafka.cluster.Broker;
import kafka.cluster.Replica;
import kafka.controller.ctrl.ControllerContext;
import kafka.controller.ctrl.PartitionAndReplica;
import kafka.func.*;
import kafka.log.TopicAndPartition;
import kafka.network.BlockingChannel;
import kafka.network.Receive;
import kafka.server.KafkaConfig;
import kafka.utils.Logging;
import kafka.utils.Sc;
import kafka.utils.ShutdownableThread;
import kafka.utils.Utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author zhoulf
 * @create 2017-11-02 04 14
 **/

public class ControllerChannelManager extends Logging {
    private ControllerContext controllerContext;
    public KafkaConfig config;

    public ControllerChannelManager(ControllerContext controllerContext, KafkaConfig config) {
        this.controllerContext = controllerContext;
        this.config = config;
        this.logIdent = "<Channel manager on controller " + config.brokerId + ">: ";
        controllerContext.liveBrokers().forEach(b -> addNewBroker(b));
    }

    private Map<Integer, ControllerBrokerStateInfo> brokerStateInfo = Maps.newHashMap();
    private Object brokerLock = new Object();

    public void startup() {
        synchronized (brokerLock) {
            brokerStateInfo.forEach((broker, info) -> startRequestSendThread(broker));
        }
    }

    public void shutdown() {
        synchronized (brokerLock) {
            brokerStateInfo.forEach((broker, info) -> removeExistingBroker(broker));
        }
    }

    public void sendRequest(Integer brokerId, RequestOrResponse request, ActionP<RequestOrResponse> callback) {
        synchronized (brokerLock) {
            ControllerBrokerStateInfo stateInfoOpt = brokerStateInfo.get(brokerId);
            Sc.match(stateInfoOpt, stateInfo -> stateInfo.messageQueue.put((request, callback)),
                    () -> warn(String.format("Not sending request %s to broker %d, since it is offline.", request, brokerId)))
            ;
        }
    }

    public void addBroker(Broker broker) {
        // be careful here. Maybe the startup() API has already started the request send thread;
        synchronized (brokerLock) {
            if (!brokerStateInfo.containsKey(broker.id)) {
                addNewBroker(broker);
                startRequestSendThread(broker.id);
            }
        }
    }

    public void removeBroker(Integer brokerId) {
        synchronized (brokerLock) {
            removeExistingBroker(brokerId);
        }
    }

    private void addNewBroker(Broker broker) {
        LinkedBlockingQueue<Tuple<RequestOrResponse, ActionP<RequestOrResponse>>> messageQueue = new LinkedBlockingQueue<>(config.controllerMessageQueueSize);
        debug(String.format("Controller %d trying to connect to broker %d", config.brokerId, broker.id));
        BlockingChannel channel = new BlockingChannel(broker.host, broker.port,
                BlockingChannel.UseDefaultBufferSize,
                BlockingChannel.UseDefaultBufferSize,
                config.controllerSocketTimeoutMs);
        RequestSendThread requestThread = new RequestSendThread(config.brokerId, controllerContext, broker, messageQueue, channel);
        requestThread.setDaemon(false);
        brokerStateInfo.put(broker.id, new ControllerBrokerStateInfo(channel, broker, messageQueue, requestThread));
    }

    private void removeExistingBroker(Integer brokerId) {
        try {
            brokerStateInfo.get(brokerId).channel.disconnect();
            brokerStateInfo.get(brokerId).messageQueue.clear();
            brokerStateInfo.get(brokerId).requestSendThread.shutdown();
            brokerStateInfo.remove(brokerId);
        } catch (Throwable e) {
            error("Error while removing broker by the controller", e);
        }
    }

    private void startRequestSendThread(Integer brokerId) {
        RequestSendThread requestThread = brokerStateInfo.get(brokerId).requestSendThread;
        if (requestThread.getState() == Thread.State.NEW)
            requestThread.start();
    }
}

class RequestSendThread extends ShutdownableThread {
    //(String.format("Controller-%d-to-broker-%d-send-thread",controllerId, toBroker.id))
    public Integer controllerId;
    public ControllerContext controllerContext;
    public Broker toBroker;
    public BlockingQueue<Tuple<RequestOrResponse, ActionP<RequestOrResponse>>> queue;
    public BlockingChannel channel;

    public RequestSendThread(String name, Boolean isInterruptible, Integer controllerId, ControllerContext controllerContext, Broker toBroker, BlockingQueue<Tuple<RequestOrResponse, ActionP<RequestOrResponse>>> queue, BlockingChannel channel) {
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
    public void doWork() {
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
                request.requestId.get match {
                    case RequestKeys.LeaderAndIsrKey =>
                        response = LeaderAndIsrResponse.readFrom(receive.buffer());
                    case RequestKeys.StopReplicaKey =>
                        response = StopReplicaResponse.readFrom(receive.buffer());
                    case RequestKeys.UpdateMetadataKey =>
                        response = UpdateMetadataResponse.readFrom(receive.buffer());
                }
                stateChangeLogger.trace(String.format("Controller %d epoch %d received response %s for a request sent to broker %s",
                        controllerId, controllerContext.epoch, response.toString(), toBroker.toString()));

                if (callback != null) {
                    callback.invoke(response);
                }
            }
        } catch (Throwable e) {
            logger.error(String.format("Controller %d fails to send a request to broker %s", controllerId, toBroker.toString()), e)
            // If there is any socket error (eg, socket timeout), the channel is no longer usable and needs to be recreated.;
            channel.disconnect();
        }
    }

    private void connectToBroker(Broker broker, BlockingChannel channel) {
        try {
            channel.connect();
            logger.info(String.format("Controller %d connected to %s for sending state change requests", controllerId, broker.toString()))
        } catch (Throwable e) {
            channel.disconnect();
            logger.error(String.format("Controller %d's connection to broker %s was unsuccessful", controllerId, broker.toString()), e)
        }
    }
}

class ControllerBrokerRequestBatch extends Logging {
    public KafkaController controller;

    public ControllerBrokerRequestBatch(KafkaController controller) {
        this.controller = controller;
    }

    public ControllerContext controllerContext = controller.controllerContext;
    public Integer controllerId = controller.config.brokerId;
    public String clientId = controller.clientId();
    public Map<Integer, Map<Tuple<String, Integer>, PartitionStateInfo>> leaderAndIsrRequestMap = Maps.newHashMap();
    public Map<Integer, List<StopReplicaRequestInfo>> stopReplicaRequestMap = Maps.newHashMap();
    public Map<Integer, HashMap<TopicAndPartition, PartitionStateInfo>> updateMetadataRequestMap = Maps.newHashMap();
    private StateChangeLogger stateChangeLogger = KafkaController.stateChangeLogger;

    public void newBatch() {
        // raise error if the previous batch is not empty;
        if (leaderAndIsrRequestMap.size() > 0)
            throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating " +
                    String.format("a new one. Some LeaderAndIsr state changes %s might be lost ", leaderAndIsrRequestMap.toString()))
        if (stopReplicaRequestMap.size() > 0)
            throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +
                    String.format("new one. Some StopReplica state changes %s might be lost ", stopReplicaRequestMap.toString()))
        if (updateMetadataRequestMap.size() > 0)
            throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +
                    String.format("new one. Some UpdateMetadata state changes %s might be lost ", updateMetadataRequestMap.toString()))
    }

    public Set<TopicAndPartition> addLeaderAndIsrRequestForBrokers(List<Integer> brokerIds, String topic, Integer partition,
                                                                   LeaderIsrAndControllerEpoch leaderIsrAndControllerEpoch,
                                                                   List<Integer> replicas, ActionP<RequestOrResponse> callback) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Sc.filter(brokerIds, b -> b >= 0).forEach(brokerId -> {
            leaderAndIsrRequestMap.getOrDefault(brokerId, new HashMap<Tuple<String, Integer>, PartitionStateInfo>());
            leaderAndIsrRequestMap.get(brokerId).put(Tuple.of(topic, partition),
                    new PartitionStateInfo(leaderIsrAndControllerEpoch, Sc.toSet(replicas)));
        });

        addUpdateMetadataRequestForBrokers(controllerContext.liveOrShuttingDownBrokerIds())
        return Sets.newHashSet(topicAndPartition);
    }

    public void addStopReplicaRequestForBrokers(List<Integer> brokerIds, String topic, Integer partition, Boolean deletePartition, ActionP2<RequestOrResponse, Integer> callback) {
        Sc.filter(brokerIds, b -> b >= 0).forEach(brokerId -> {
            stopReplicaRequestMap.getOrDefault(brokerId, Lists.newArrayList());
            List<StopReplicaRequestInfo> v = stopReplicaRequestMap.get(brokerId);
            if (callback != null)
                stopReplicaRequestMap.get(brokerId).add(new StopReplicaRequestInfo(new PartitionAndReplica(topic, partition, brokerId),
                        deletePartition, r -> callback.invoke(r, brokerId)));
            else
                stopReplicaRequestMap.get(brokerId).add(new StopReplicaRequestInfo(new PartitionAndReplica(topic, partition, brokerId), deletePartition);
        });
    }

    /**
     * Send UpdateMetadataRequest to the given brokers for the given partitions and partitions that are being deleted
     */
    public void updateMetadataRequestMapFor(List<Integer> brokerIds, TopicAndPartition partition, Boolean beingDeleted) {
        LeaderIsrAndControllerEpoch leaderIsrAndControllerEpoch = controllerContext.partitionLeadershipInfo.get(partition);
        if (leaderIsrAndControllerEpoch != null) {
            Set<Integer> replicas = Sc.toSet(controllerContext.partitionReplicaAssignment.get(partition));
            PartitionStateInfo partitionStateInfo;
            if (beingDeleted) {
                LeaderAndIsr leaderAndIsr = new LeaderAndIsr(LeaderAndIsr.LeaderDuringDelete, leaderIsrAndControllerEpoch.leaderAndIsr.isr);
                partitionStateInfo = new PartitionStateInfo(new LeaderIsrAndControllerEpoch(leaderAndIsr, leaderIsrAndControllerEpoch.controllerEpoch), replicas);
            } else {
                partitionStateInfo = new PartitionStateInfo(leaderIsrAndControllerEpoch, replicas);
            }
            Sc.filter(brokerIds, b -> b >= 0).forEach(
                    brokerId -> {
                        updateMetadataRequestMap.getOrDefault(brokerId, new HashMap<TopicAndPartition, PartitionStateInfo>());
                        updateMetadataRequestMap.get(brokerId).put(partition, partitionStateInfo);
                    });
        } else {
            info(String.format("Leader not yet assigned for partition %s. Skip sending UpdateMetadataRequest.", partition));
        }
    }

    public void addUpdateMetadataRequestForBrokers(List<Integer> brokerIds, Set<TopicAndPartition> partitions, ActionP<RequestOrResponse> callback) {
        Set<TopicAndPartition> filteredPartitions;
        Set<TopicAndPartition> givenPartitions;
        if (partitions.isEmpty())
            givenPartitions = controllerContext.partitionLeadershipInfo.keySet();
        else
            givenPartitions = partitions;
        if (controller.deleteTopicManager.partitionsToBeDeleted.isEmpty)
            filteredPartitions = givenPartitions;
        else
            filteredPartitions = (givenPartitions-- controller.deleteTopicManager.partitionsToBeDeleted);
        filteredPartitions.forEach(partition -> updateMetadataRequestMapFor(brokerIds, partition, false));
        controller.deleteTopicManager.partitionsToBeDeleted.forEach(partition -> updateMetadataRequestMapFor(partition, true));
    }

    public void sendRequestsToBrokers(Integer controllerEpoch, Integer correlationId) {
        leaderAndIsrRequestMap.forEach((broker, partitionStateInfos) -> {
            Set<Integer> leaderIds = Sc.toSet(Sc.map(partitionStateInfos, info -> info.getValue().leaderIsrAndControllerEpoch.leaderAndIsr.leader));
            Set<Broker> leaders = Sc.filter(controllerContext.liveOrShuttingDownBrokers(), b -> leaderIds.contains(b.id));
            LeaderAndIsrRequest leaderAndIsrRequest = new LeaderAndIsrRequest(partitionStateInfos, leaders, controllerId, controllerEpoch, correlationId, clientId);
            partitionStateInfos.forEach((k, v) -> {
                String typeOfRequest = (broker == v.leaderIsrAndControllerEpoch.leaderAndIsr.leader) ? "become-leader" : "become-follower";
                stateChangeLogger.trace(String.format("Controller %d epoch %d sending %s LeaderAndIsr request %s with correlationId %d to broker %d " +
                                "for partition <%s,%d>", controllerId, controllerEpoch, typeOfRequest,
                        v.leaderIsrAndControllerEpoch, correlationId, broker,
                        k.v1, k.v2));
            });
            controller.sendRequest(broker, leaderAndIsrRequest, null);
        });
        val leaderIds = partitionStateInfos.map(_._2.leaderIsrAndControllerEpoch.leaderAndIsr.leader).toSet;
        val leaders = controllerContext.liveOrShuttingDownBrokers.filter(b = > leaderIds.contains(b.id));
        val leaderAndIsrRequest = new LeaderAndIsrRequest(partitionStateInfos, leaders, controllerId, controllerEpoch, correlationId, clientId);
        for (p< -partitionStateInfos) {
            val typeOfRequest = if (broker == p._2.leaderIsrAndControllerEpoch.leaderAndIsr.leader) "become-leader"
            else "become-follower";
            stateChangeLogger.trace(("Controller %d epoch %d sending %s LeaderAndIsr request %s with correlationId %d to broker %d " +
                    "for partition <%s,%d>").format(controllerId, controllerEpoch, typeOfRequest,
                    p._2.leaderIsrAndControllerEpoch, correlationId, broker,
                    p._1._1, p._1._2));
        }
        controller.sendRequest(broker, leaderAndIsrRequest, null);
    }
        leaderAndIsrRequestMap.clear();
    updateMetadataRequestMap.foreach

    {
        m =>
        val broker = m._1;
        val partitionStateInfos = m._2.toMap;
        val updateMetadataRequest = new UpdateMetadataRequest(controllerId, controllerEpoch, correlationId, clientId,
                partitionStateInfos, controllerContext.liveOrShuttingDownBrokers);
        partitionStateInfos.foreach(p = > stateChangeLogger.trace(("Controller %d epoch %d sending UpdateMetadata request %s with " +
                "correlationId %d to broker %d for partition %s").format(controllerId, controllerEpoch, p._2.leaderIsrAndControllerEpoch,
                correlationId, broker, p._1)));
        controller.sendRequest(broker, updateMetadataRequest, null);
    }
        updateMetadataRequestMap.clear();
    stopReplicaRequestMap foreach

    {
        case (broker,replicaInfoList)=>
            val stopReplicaWithDelete = replicaInfoList.filter(p = > p.deletePartition == true).map(i = > i.replica).toSet;
            val stopReplicaWithoutDelete = replicaInfoList.filter(p = > p.deletePartition == false).map(i = > i.replica).toSet;
            debug("The stop replica request (delete = true) sent to broker %d is %s";
        .format(broker, stopReplicaWithDelete.mkString(",")))
            debug("The stop replica request (delete = false) sent to broker %d is %s";
        .format(broker, stopReplicaWithoutDelete.mkString(",")))
            replicaInfoList.foreach {
            r =>
            val stopReplicaRequest = new StopReplicaRequest(r.deletePartition,
                    Set(TopicAndPartition(r.replica.topic, r.replica.partition)), controllerId, controllerEpoch, correlationId);
            controller.sendRequest(broker, stopReplicaRequest, r.callback);
        }
    }
        stopReplicaRequestMap.clear();
}
}

class ControllerBrokerStateInfo {
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

class StopReplicaRequestInfo {
    public PartitionAndReplica replica;
    public Boolean deletePartition;
    public ActionP<RequestOrResponse> callback;

    public StopReplicaRequestInfo(PartitionAndReplica replica, Boolean deletePartition) {
        this(replica, deletePartition, null);
    }

    public StopReplicaRequestInfo(PartitionAndReplica replica, Boolean deletePartition, ActionP<RequestOrResponse> callback) {
        this.replica = replica;
        this.deletePartition = deletePartition;
        this.callback = callback;
    }
}

class Callbacks {
    public ActionP<RequestOrResponse> leaderAndIsrResponseCallback;
    public ActionP<RequestOrResponse> updateMetadataResponseCallback;
    public ActionP<RequestOrResponse> stopReplicaResponseCallback;

    public Callbacks(ActionP<RequestOrResponse> leaderAndIsrResponseCallback, ActionP<RequestOrResponse> updateMetadataResponseCallback, ActionP<RequestOrResponse> stopReplicaResponseCallback) {
        this.leaderAndIsrResponseCallback = leaderAndIsrResponseCallback;
        this.updateMetadataResponseCallback = updateMetadataResponseCallback;
        this.stopReplicaResponseCallback = stopReplicaResponseCallback;
    }

    class CallbackBuilder {
        ActionP<RequestOrResponse> leaderAndIsrResponseCbk;
        ActionP<RequestOrResponse> updateMetadataResponseCbk;
        ActionP<RequestOrResponse> stopReplicaResponseCbk;

        public CallbackBuilder leaderAndIsrCallback(ActionP<RequestOrResponse> cbk) {
            leaderAndIsrResponseCbk = cbk;
            return this;
        }

        public CallbackBuilder updateMetadataCallback(ActionP<RequestOrResponse> cbk) {
            updateMetadataResponseCbk = cbk;
            return this;
        }

        public CallbackBuilder stopReplicaCallback(ActionP<RequestOrResponse> cbk) {
            stopReplicaResponseCbk = cbk;
            return this;
        }

        public Callbacks build() {
            return new Callbacks(leaderAndIsrResponseCbk, updateMetadataResponseCbk, stopReplicaResponseCbk);
        }
    }
}
