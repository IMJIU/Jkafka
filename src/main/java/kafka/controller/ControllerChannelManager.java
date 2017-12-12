package kafka.controller;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import kafka.api.*;
import kafka.cluster.Broker;
import kafka.cluster.Replica;
import kafka.controller.channel.ControllerBrokerStateInfo;
import kafka.controller.channel.RequestSendThread;
import kafka.controller.ctrl.ControllerContext;
import kafka.controller.ctrl.LeaderIsrAndControllerEpoch;
import kafka.controller.ctrl.PartitionAndReplica;
import kafka.func.*;
import kafka.log.TopicAndPartition;
import kafka.network.BlockingChannel;
import kafka.network.Receive;
import kafka.server.KafkaConfig;
import kafka.server.StateChangeLogger;
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
            ControllerBrokerStateInfo stateInfo = brokerStateInfo.get(brokerId);
            if (stateInfo != null) {
                try {
                    stateInfo.messageQueue.put(Tuple.of(request, callback));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } else {
                warn(String.format("Not sending request %s to broker %d, since it is offline.", request, brokerId));
            }
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


class ControllerBrokerRequestBatch extends Logging {
    public KafkaController controller;

    public ControllerBrokerRequestBatch(KafkaController controller) {
        this.controller = controller;
        controllerContext = controller.controllerContext;
        controllerId = controller.config.brokerId;
        clientId = controller.clientId();
    }

    public ControllerContext controllerContext;
    public Integer controllerId;
    public String clientId;
    public Map<Integer, Map<Tuple<String, Integer>, PartitionStateInfo>> leaderAndIsrRequestMap = Maps.newHashMap();
    public Map<Integer, List<StopReplicaRequestInfo>> stopReplicaRequestMap = Maps.newHashMap();
    public Map<Integer, HashMap<TopicAndPartition, PartitionStateInfo>> updateMetadataRequestMap = Maps.newHashMap();
    private StateChangeLogger stateChangeLogger = KafkaController.stateChangeLogger;

    public void newBatch() {
        // raise error if the previous batch is not empty;
        if (leaderAndIsrRequestMap.size() > 0)
            throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating " +
                    String.format("a new one. Some LeaderAndIsr state changes %s might be lost ", leaderAndIsrRequestMap.toString()));
        if (stopReplicaRequestMap.size() > 0)
            throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +
                    String.format("new one. Some StopReplica state changes %s might be lost ", stopReplicaRequestMap.toString()));
        if (updateMetadataRequestMap.size() > 0)
            throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +
                    String.format("new one. Some UpdateMetadata state changes %s might be lost ", updateMetadataRequestMap.toString()));
    }

    public void addLeaderAndIsrRequestForBrokers(List<Integer> brokerIds, String topic, Integer partition,
                                                 LeaderIsrAndControllerEpoch leaderIsrAndControllerEpoch,
                                                 List<Integer> replicas) {
        addLeaderAndIsrRequestForBrokers(brokerIds, topic, partition, leaderIsrAndControllerEpoch, replicas, null);
    }

    public void addLeaderAndIsrRequestForBrokers(List<Integer> brokerIds, String topic, Integer partition,
                                                 LeaderIsrAndControllerEpoch leaderIsrAndControllerEpoch,
                                                 List<Integer> replicas, ActionP<RequestOrResponse> callback) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Sc.filter(brokerIds, b -> b >= 0).forEach(brokerId -> {
            leaderAndIsrRequestMap.getOrDefault(brokerId, new HashMap<>());
            leaderAndIsrRequestMap.get(brokerId).put(Tuple.of(topic, partition),
                    new PartitionStateInfo(leaderIsrAndControllerEpoch, Sc.toSet(replicas)));
        });

        addUpdateMetadataRequestForBrokers(Sc.toList(controllerContext.liveOrShuttingDownBrokerIds()), Sets.newHashSet(topicAndPartition), null);
    }

    public void addStopReplicaRequestForBrokers(List<Integer> brokerIds, String topic, Integer partition, Boolean deletePartition, ActionP2<RequestOrResponse, Integer> callback) {
        Sc.filter(brokerIds, b -> b >= 0).forEach(brokerId -> {
            stopReplicaRequestMap.getOrDefault(brokerId, Lists.newArrayList());
            List<StopReplicaRequestInfo> v = stopReplicaRequestMap.get(brokerId);
            if (callback != null)
                stopReplicaRequestMap.get(brokerId).add(new StopReplicaRequestInfo(new PartitionAndReplica(topic, partition, brokerId),
                        deletePartition, r -> callback.invoke(r, brokerId)));
            else
                stopReplicaRequestMap.get(brokerId).add(new StopReplicaRequestInfo(new PartitionAndReplica(topic, partition, brokerId), deletePartition));
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
        if (controller.deleteTopicManager.partitionsToBeDeleted.isEmpty())
            filteredPartitions = givenPartitions;
        else
            filteredPartitions = Sc.subtract(givenPartitions, controller.deleteTopicManager.partitionsToBeDeleted);
        filteredPartitions.forEach(partition -> updateMetadataRequestMapFor(brokerIds, partition, false));
        controller.deleteTopicManager.partitionsToBeDeleted.forEach(partition -> updateMetadataRequestMapFor(brokerIds, partition, true));
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
        leaderAndIsrRequestMap.clear();
        stopReplicaRequestMap.forEach((broker, replicaInfoList) -> {
            Set<PartitionAndReplica> stopReplicaWithDelete = Sc.toSet(Sc.map(Sc.filter(replicaInfoList, p -> p.deletePartition == true), i -> i.replica));
            Set<PartitionAndReplica> stopReplicaWithoutDelete = Sc.toSet(Sc.map(Sc.filter(replicaInfoList, p -> p.deletePartition == false), i -> i.replica));
            debug(String.format("The stop replica request (delete = true) sent to broker %d is %s", broker, stopReplicaWithDelete));
            debug(String.format("The stop replica request (delete = false) sent to broker %d is %s", broker, stopReplicaWithoutDelete));
            replicaInfoList.forEach(r -> {
                StopReplicaRequest stopReplicaRequest = new StopReplicaRequest(r.deletePartition,
                        Sets.newHashSet(new TopicAndPartition(r.replica.topic, r.replica.partition)), controllerId, controllerEpoch, correlationId);
                controller.sendRequest(broker, stopReplicaRequest, r.callback);
            });
        });
        stopReplicaRequestMap.clear();
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
}

