package kafka.utils;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import kafka.admin.PreferredReplicaLeaderElectionCommand;
import kafka.api.LeaderAndIsr;
import kafka.controller.KafkaController;
import kafka.controller.ctrl.LeaderIsrAndControllerEpoch;
import kafka.cluster.Broker;
import kafka.cluster.Cluster;
import kafka.common.AdminOperationException;
import kafka.common.KafkaException;
import kafka.common.NoEpochForPartitionException;
import kafka.consumer.ConsumerThreadId;
import kafka.consumer.TopicCount;
import kafka.controller.ReassignedPartitionsContext;
import kafka.func.Checker;
import kafka.func.Handler;
import kafka.func.Tuple;
import kafka.func.Tuple3;
import kafka.log.TopicAndPartition;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkBadVersionException;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.zookeeper.data.Stat;

import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ZkUtils {
    private static final Logging log = Logging.getLogger(ZkUtils.class.getName());
    public static final String ConsumersPath = "/consumers";
    public static final String BrokerIdsPath = "/brokers/ids";
    public static final String BrokerTopicsPath = "/brokers/topics";
    public static final String TopicConfigPath = "/config/topics";
    public static final String TopicConfigChangesPath = "/config/changes";
    public static final String ControllerPath = "/controller";
    public static final String ControllerEpochPath = "/controller_epoch";
    public static final String ReassignPartitionsPath = "/admin/reassign_partitions";
    public static final String DeleteTopicsPath = "/admin/delete_topics";
    public static final String PreferredReplicaLeaderElectionPath = "/admin/preferred_replica_election";

    public static String getTopicPath(String topic) {
        return BrokerTopicsPath + "/" + topic;
    }


    public static String getTopicPartitionsPath(String topic) {
        return getTopicPath(topic) + "/partitions";
    }

    public static String getTopicConfigPath(String topic) {
        return TopicConfigPath + "/" + topic;
    }

    public static String getDeleteTopicPath(String topic) {
        return DeleteTopicsPath + "/" + topic;
    }

    public static Integer getController(ZkClient zkClient) throws Throwable {
        Tuple<Optional<String>, Stat> t = readDataMaybeNull(zkClient, ControllerPath);
        if (t.v1.isPresent()) {
            return KafkaController.parseControllerId(t.v1.get());
        }
        throw new KafkaException("Controller doesn't exist");
    }

    public static String getTopicPartitionPath(String topic, Integer partitionId) {
        return getTopicPartitionsPath(topic) + "/" + partitionId;
    }


    public static String getTopicPartitionLeaderAndIsrPath(String topic, Integer partitionId) {
        return getTopicPartitionPath(topic, partitionId) + "/" + "state";
    }


    public static List<Integer> getSortedBrokerList(ZkClient zkClient) {
        return Sc.sorted(Sc.map(ZkUtils.getChildren(zkClient, BrokerIdsPath), s -> Integer.parseInt(s)));
    }


    public static List<Broker> getAllBrokersInCluster(ZkClient zkClient) {
        Stream<String> brokerIds = ZkUtils.getChildrenParentMayNotExist(zkClient, ZkUtils.BrokerIdsPath).stream().sorted();
        return brokerIds.map(i -> Integer.parseInt(i)).map(i -> getBrokerInfo(zkClient, i)).filter(i -> i.isPresent()).map(i -> i.get()).collect(Collectors.toList());
    }

    public static Optional<LeaderAndIsr> getLeaderAndIsrForPartition(ZkClient zkClient, String topic, Integer partition) {
        return Sc.map(ReplicationUtils.getLeaderIsrAndEpochForPartition(zkClient, topic, partition), l -> l.leaderAndIsr);
    }

    public static void setupCommonPaths(ZkClient zkClient) {
        for (String path : Lists.newArrayList(ConsumersPath, BrokerIdsPath, BrokerTopicsPath, TopicConfigChangesPath, TopicConfigPath, DeleteTopicsPath))
            makeSurePersistentPathExists(zkClient, path);
    }

    public static Optional<Integer> getLeaderForPartition(ZkClient zkClient, String topic, Integer partition) {
        Optional<String> leaderAndIsrOpt = readDataMaybeNull(zkClient, getTopicPartitionLeaderAndIsrPath(topic, partition)).v1;
        if (leaderAndIsrOpt.isPresent()) {
            String leaderAndIsr = leaderAndIsrOpt.get();
            Map<String, Object> m = JSON.parseObject(leaderAndIsr, Map.class);
            if (m != null) {
                return Optional.of(Integer.parseInt(m.get("leader").toString()));
            }
        }
        return Optional.empty();
    }

    /**
     * This API should read the epoch in the ISR path. It is sufficient to read the epoch in the ISR path, since if the
     * leader fails after updating epoch in the leader path and before updating epoch in the ISR path, effectively some
     * other broker will retry becoming leader with the same new epoch value.
     */
    public static Integer getEpochForPartition(ZkClient zkClient, String topic, Integer partition) throws Throwable {
        Optional<String> leaderAndIsrOpt = readDataMaybeNull(zkClient, getTopicPartitionLeaderAndIsrPath(topic, partition)).v1;
        if (leaderAndIsrOpt.isPresent()) {
            String leaderAndIsr = leaderAndIsrOpt.get();
            Map<String, Object> m = JSON.parseObject(leaderAndIsr, Map.class);
            if (m != null) {
                return Integer.parseInt(m.get("leader_epoch").toString());
            }
            throw new NoEpochForPartitionException(String.format("No epoch, leaderAndISR data for partition <%s,%d> is invalid", topic, partition));
        }
        throw new NoEpochForPartitionException(String.format("No epoch, ISR path for partition <%s,%d> is empty", topic, partition));
    }

    /**
     * Gets the in-sync replicas (ISR) for a specific topic and partition
     */
    public static List<Integer> getInSyncReplicasForPartition(ZkClient zkClient, String topic, Integer partition) {
        Optional<String> leaderAndIsrOpt = readDataMaybeNull(zkClient, getTopicPartitionLeaderAndIsrPath(topic, partition)).v1;
        if (leaderAndIsrOpt.isPresent()) {
            String leaderAndIsr = leaderAndIsrOpt.get();
            Map<String, Object> m = JSON.parseObject(leaderAndIsr, Map.class);
            if (m != null) {
                return (List<Integer>) (m.get("isr"));
            }
        }
        return Collections.emptyList();
    }

    /**
     * Gets the assigned replicas (AR) for a specific topic and partition
     */
    public static List<Integer> getReplicasForPartition(ZkClient zkClient, String topic, Integer partition) {
        Optional<String> jsonPartitionMapOpt = readDataMaybeNull(zkClient, getTopicPath(topic)).v1;
        if (jsonPartitionMapOpt.isPresent()) {
            String leaderAndIsr = jsonPartitionMapOpt.get();
            Map<String, Object> m = JSON.parseObject(leaderAndIsr, Map.class);
            if (m != null) {
                Object partitions = (m.get("partitions"));
                if (partitions != null) {
                    List<Integer> seq = ((Map<String, List<Integer>>) partitions).get(partition.toString());
                    if (seq != null) {
                        return seq;
                    }
                }
            }
        }
        return Collections.emptyList();
    }

    public static void registerBrokerInZk(ZkClient zkClient, Integer id, String host, Integer port, Integer timeout, Integer jmxPort) throws Throwable {
        String brokerIdPath = ZkUtils.BrokerIdsPath + "/" + id;
        String timestamp = Time.get().milliseconds().toString();
        String brokerInfo = JSON.toJSONString(ImmutableMap.of("version", 1, "host", host, "port", port, "jmx_port", jmxPort, "timestamp", timestamp));
        Broker expectedBroker = new Broker(id, host, port);

        try {
            createEphemeralPathExpectConflictHandleZKBug(zkClient, brokerIdPath, brokerInfo, expectedBroker,
                    (brokerString, broker) -> Broker.createBroker(((Broker) broker).id, brokerString).equals((Broker) broker),
                    timeout);

        } catch (ZkNodeExistsException e) {
            throw new RuntimeException("A broker is already registered on the path " + brokerIdPath
                    + ". This probably " + "indicates that you either have configured a brokerid that is already in use, or "
                    + "else you have shutdown this broker and restarted it faster than the zookeeper "
                    + "timeout so it appears to be re-registering.");
        }
        log.info(String.format("Registered broker %d at path %s with address %s:%d.", id, brokerIdPath, host, port));
    }

    public static String getConsumerPartitionOwnerPath(String group, String topic, Integer partition) {
        ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs(group, topic);
        return topicDirs.consumerOwnerDir() + "/" + partition;
    }


    public static String leaderAndIsrZkData(LeaderAndIsr leaderAndIsr, Integer controllerEpoch) {
        return JSON.toJSONString(ImmutableMap.of("version", 1, "leader", leaderAndIsr.leader, "leader_epoch", leaderAndIsr.leaderEpoch,
                "controller_epoch", controllerEpoch, "isr", leaderAndIsr.isr));
    }

    /**
     * Get JSON partition to replica map from zookeeper.
     */
    public static String replicaAssignmentZkData(Map<String, List<Integer>> map) {
        return JSON.toJSONString(ImmutableMap.of("version", 1, "partitions", map));
    }

    /**
     * make sure a persistent path exists in ZK. Create the path if not exist.
     */
    public static void makeSurePersistentPathExists(ZkClient client, String path) {
        if (!client.exists(path))
            client.createPersistent(path, true); // won't throw NoNodeException or NodeExistsException;
    }

    /**
     * create the parent path
     */
    private static void createParentPath(ZkClient client, String path) {
        String parentDir = path.substring(0, path.lastIndexOf('/'));
        if (parentDir.length() != 0)
            client.createPersistent(parentDir, true);
    }

    /**
     * Create an ephemeral node with the given path and data. Create parents if necessary.
     */
    private static void createEphemeralPath(ZkClient client, String path, String data) {
        try {
            client.createEphemeral(path, data);
        } catch (ZkNoNodeException e) {
            createParentPath(client, path);
            client.createEphemeral(path, data);
        }
    }


    /**
     * Create an ephemeral node with the given path and data.
     * Throw NodeExistException if node already exists.
     */
    public static void createEphemeralPathExpectConflict(ZkClient client, String path, String data) {
        try {
            createEphemeralPath(client, path, data);
        } catch (ZkNodeExistsException e) {
            // this can happen when there is connection loss; make sure the data is what we intend to write;
            String storedData = null;
            try {
                storedData = readData(client, path).v1;
            } catch (ZkNoNodeException e1) {
                // the node disappeared; treat as if node existed and let caller handles this;
            } catch (Throwable e2) {
                throw e2;
            }
            if (storedData == null || storedData != data) {
                log.info("conflict in " + path + " data: " + data + " stored data: " + storedData);
                throw e;
            } else {
                // otherwise, the creation succeeded, return normally;
                log.info(path + " exists with value " + data + " during connection loss; this is ok");
            }
        } catch (Throwable e2) {
            throw e2;
        }
    }

    /**
     * Create an ephemeral node with the given path and data.
     * Throw NodeExistsException if node already exists.
     * Handles the following ZK session timeout bug:
     * <p>
     * https://issues.apache.org/jira/browse/ZOOKEEPER-1740
     * <p>
     * Upon receiving a NodeExistsException, read the data from the conflicted path and
     * trigger the checker function comparing the read data and the expected data,
     * If the checker function returns true then the above bug might be encountered, back off and retry;
     * otherwise re-throw the exception
     */
    public static void createEphemeralPathExpectConflictHandleZKBug(ZkClient zkClient, String path, String data,
                                                                    Object expectedCallerData,
                                                                    Checker<String, Object, Boolean> checker,
                                                                    Integer backoffTime) {
        while (true) {
            try {
                createEphemeralPathExpectConflict(zkClient, path, data);
                return;
            } catch (ZkNodeExistsException e) {
                // An ephemeral node may still exist even after its corresponding session has expired;
                // due to a Zookeeper bug, in this case we need to retry writing until the previous node is deleted;
                // and hence the write succeeds without ZkNodeExistsException;
                Optional<String> opt = ZkUtils.readDataMaybeNull(zkClient, path).v1;
                if (opt.isPresent()) {
                    String writtenData = opt.get();
                    if (checker.check(writtenData, expectedCallerData)) {
                        log.info(String.format("I wrote this conflicted ephemeral node <%s> at %s a while back in a different session, ", data, path)
                                + "hence I will backoff for this node to be deleted by Zookeeper and retry");
                        try {
                            Thread.sleep(backoffTime);
                        } catch (InterruptedException e1) {
                            throw new RuntimeException(e1);
                        }
                    } else {
                        throw e;
                    }
                }
                // the node disappeared; retry creating the ephemeral node immediately
            } catch (Throwable e2) {
                throw e2;
            }
        }
    }

    /**
     * Create an persistent node with the given path and data. Create parents if necessary.
     */
    public static void createPersistentPath(ZkClient client, String path) {
        createPersistentPath(client, path, "");
    }

    /**
     * Create an persistent node with the given path and data. Create parents if necessary.
     */
    public static void createPersistentPath(ZkClient client, String path, String data) {
        try {
            client.createPersistent(path, data);
        } catch (ZkNoNodeException e) {
            createParentPath(client, path);
            client.createPersistent(path, data);
        }
    }

    public static String createSequentialPersistentPath(ZkClient client, String path) {
        return client.createPersistentSequential(path, "");
    }

    public static String createSequentialPersistentPath(ZkClient client, String path, String data) {
        return client.createPersistentSequential(path, data);
    }

    /**
     * Update the value of a persistent node with the given path and data.
     * create parrent directory if necessary. Never throw NodeExistException.
     * Return the updated path zkVersion
     */
    public static void updatePersistentPath(ZkClient client, String path, String data) {
        try {
            client.writeData(path, data);
        } catch (ZkNoNodeException e) {
            createParentPath(client, path);
            try {
                client.createPersistent(path, data);
            } catch (ZkNodeExistsException e1) {
                client.writeData(path, data);
            } catch (Throwable e2) {
                throw e2;
            }
        } catch (Throwable e2) {
            throw e2;
        }
    }

    /**
     * Conditional update the persistent path data, return (true, newVersion) if it succeeds, otherwise (the path doesn't
     * exist, the current version is not the expected version, etc.) return (false, -1)
     * <p>
     * When there is a ConnectionLossException during the conditional update, zkClient will retry the update and may fail
     * since the previous update may have succeeded (but the stored zkVersion no longer matches the expected one).
     * In this case, we will run the optionalChecker to further check if the previous write did indeed succeeded.
     */
    public static Tuple<Boolean, Integer> conditionalUpdatePersistentPath(ZkClient client, String path, String data, Integer expectVersion) {
        return conditionalUpdatePersistentPath(client, path, data, expectVersion, Optional.empty());
    }

    public static Tuple<Boolean, Integer> conditionalUpdatePersistentPath(ZkClient client, String path, String data, Integer expectVersion,
                                                                          Optional<Handler<Tuple3<ZkClient, String, String>, Tuple<Boolean, Integer>>> optionalChecker) {
        try {
            Stat stat = client.writeDataReturnStat(path, data, expectVersion);
            log.debug(String.format("Conditional update of path %s with value %s and expected version %d succeeded, returning the new version: %d",
                    path, data, expectVersion, stat.getVersion()));
            return Tuple.of(true, stat.getVersion());
        } catch (ZkBadVersionException e1) {
            if (optionalChecker.isPresent()) {
                return optionalChecker.get().handle(Tuple3.of(client, path, data));
            } else {
                log.debug("Checker method is not passed skipping zkData match");
            }
            log.warn(String.format("Conditional update of path %s with data %s and expected version %d failed due to %s", path, data,
                    expectVersion, e1.getMessage()));
            return Tuple.of(false, -1);
        } catch (Exception e2) {
            log.warn(String.format("Conditional update of path %s with data %s and expected version %d failed due to %s", path, data,
                    expectVersion, e2.getMessage()));
            return Tuple.of(false, -1);
        }
    }

    /**
     * Conditional update the persistent path data, return (true, newVersion) if it succeeds, otherwise (the current
     * version is not the expected version, etc.) return (false, -1). If path doesn't exist, throws ZkNoNodeException
     */
    public static Tuple<Boolean, Integer> conditionalUpdatePersistentPathIfExists(ZkClient client, String path, String data, Integer expectVersion) {
        try {
            Stat stat = client.writeDataReturnStat(path, data, expectVersion);
            log.debug(String.format("Conditional update of path %s with value %s and expected version %d succeeded, returning the new version: %d",
                    path, data, expectVersion, stat.getVersion()));
            return Tuple.of(true, stat.getVersion());
        } catch (ZkNoNodeException nne) {
            throw nne;
        } catch (Exception e) {
            log.error(String.format("Conditional update of path %s with data %s and expected version %d failed due to %s", path, data,
                    expectVersion, e.getMessage()));
            return Tuple.of(false, -1);
        }
    }

    /**
     * Update the value of a persistent node with the given path and data.
     * create parrent directory if necessary. Never throw NodeExistException.
     */
    public static void updateEphemeralPath(ZkClient client, String path, String data) {
        try {
            client.writeData(path, data);
        } catch (ZkNoNodeException e) {
            createParentPath(client, path);
            client.createEphemeral(path, data);
        } catch (Throwable e2) {
            throw e2;
        }
    }

    public static Boolean deletePath(ZkClient client, String path) {
        try {
            client.delete(path);
        } catch (ZkNoNodeException e) {
            // this can happen during a connection loss event, return normally;
            log.info(path + " deleted during connection loss; this is ok");
            return false;
        } catch (Throwable e2) {
            throw e2;
        }
        return true;
    }

    public static void deletePathRecursive(ZkClient client, String path) {
        try {
            client.deleteRecursive(path);
        } catch (ZkNoNodeException e) {
            // this can happen during a connection loss event, return normally;
            log.info(path + " deleted during connection loss; this is ok");
        } catch (Throwable e2) {
            throw e2;
        }
    }

    public static void maybeDeletePath(String zkUrl, String dir) {
        try {
            ZkClient zk = new ZkClient(zkUrl, 30 * 1000, 30 * 1000, new ZKStringSerializer());
            zk.deleteRecursive(dir);
            zk.close();
        } catch (Throwable e) {
            // swallow;
        }
    }

    public static Tuple<String, Stat> readData(ZkClient client, String path) {
        Stat stat = new Stat();
        String dataStr = client.readData(path, stat);
        return Tuple.of(dataStr, stat);
    }

    public static Tuple<Optional<String>, Stat> readDataMaybeNull(ZkClient client, String path) {
        Stat stat = new Stat();
        try {
            return Tuple.of(Optional.of(client.readData(path, stat)), stat);
        } catch (ZkNoNodeException e) {
            return Tuple.of(Optional.empty(), stat);
        } catch (Throwable e2) {
            throw e2;
        }
    }

    public static List<String> getChildren(ZkClient client, String path) {
        // triggers implicit conversion from java list to scala Seq;
        return client.getChildren(path);
    }

    public static List<String> getChildrenParentMayNotExist(ZkClient client, String path) {
        // triggers implicit conversion from java list to scala Seq;
        try {
            return client.getChildren(path);
        } catch (ZkNoNodeException e) {
            return null;
        } catch (Throwable e2) {
            throw e2;
        }
    }

    /**
     * Check if the given path exists
     */
    public static Boolean pathExists(ZkClient client, String path) {
        return client.exists(path);
    }

    public static Cluster getCluster(ZkClient zkClient) throws Throwable {
        Cluster cluster = new Cluster();
        List<String> nodes = getChildrenParentMayNotExist(zkClient, BrokerIdsPath);
        for (String node : nodes) {
            String brokerZKString = readData(zkClient, BrokerIdsPath + "/" + node).v1;
            cluster.add(Broker.createBroker(Integer.parseInt(node), brokerZKString));
        }
        return cluster;
    }

    public static Map<TopicAndPartition, LeaderIsrAndControllerEpoch> getPartitionLeaderAndIsrForTopics(ZkClient zkClient, Set<TopicAndPartition> topicAndPartitions) throws Throwable {
        HashMap<TopicAndPartition, LeaderIsrAndControllerEpoch> ret = Maps.newHashMap();
        for (TopicAndPartition topicAndPartition : topicAndPartitions) {
            Optional<LeaderIsrAndControllerEpoch> leaderIsrAndControllerEpoch = ReplicationUtils.getLeaderIsrAndEpochForPartition(zkClient, topicAndPartition.topic, topicAndPartition.partition);
            if (leaderIsrAndControllerEpoch.isPresent())
                ret.put(topicAndPartition, leaderIsrAndControllerEpoch.get());
        }
        return ret;
    }

    public static Map<TopicAndPartition, List<Integer>> getReplicaAssignmentForTopics(ZkClient zkClient, List<String> topics) {
        Map<TopicAndPartition, List<Integer>> ret = Maps.newHashMap();
        topics.forEach(topic -> {
            Optional<String> jsonPartitionMapOpt = readDataMaybeNull(zkClient, getTopicPath(topic)).v1;
            if (jsonPartitionMapOpt.isPresent()) {
                Map<String, Object> m = JSON.parseObject(jsonPartitionMapOpt.get(), Map.class);
                if (m != null) {
                    Object repl = m.get("partitions");
                    if (repl != null) {
                        Map<String, List<Integer>> replicaMap = (Map<String, List<Integer>>) repl;
                        replicaMap.forEach((partition, replicas) -> {
                            ret.put(new TopicAndPartition(topic, Integer.parseInt(partition)), replicas);
                            log.debug(String.format("Replicas assigned to topic <%s>, partition <%s> are <%s>", topic, partition, replicas));
                        });
                    }
                }
            }
        });
        return ret;
    }

    public static Map<String, Map<Integer, List<Integer>>> getPartitionAssignmentForTopics(ZkClient zkClient, List<String> topics) {
        Map<String, Map<Integer, List<Integer>>> ret = Maps.newHashMap();
        topics.forEach(topic -> {
            Map partitionMap = null;
            Optional<String> jsonPartitionMapOpt = null;
            jsonPartitionMapOpt = readDataMaybeNull(zkClient, getTopicPath(topic)).v1;
            if (jsonPartitionMapOpt.isPresent()) {
                Map<String, Object> m = JSON.parseObject(jsonPartitionMapOpt.get(), Map.class);
                if (m != null) {
                    Object repl = m.get("partitions");
                    if (repl != null) {
                        Map<String, List<Integer>> replicaMap = (Map<String, List<Integer>>) repl;
                        partitionMap = Sc.mapKey(replicaMap, k -> Integer.parseInt(k));
                    }
                }
            }
            if (partitionMap == null) {
                partitionMap = Maps.newHashMap();
            }
            log.debug(String.format("Partition map for /brokers/topics/%s is %s", topic, partitionMap));
            ret.put(topic, partitionMap);
        });
        return ret;
    }

    public static Map<String, List<Integer>> getPartitionsForTopics(ZkClient zkClient, List<String> topics) {
        Map<String, List<Integer>> result = Maps.newHashMap();
        getPartitionAssignmentForTopics(zkClient, topics).forEach((topic, partitionMap) -> {
            log.debug(String.format("partition assignment of /brokers/topics/%s is %s", topic, partitionMap));
            // TODO: 2017/11/1    result.put(topic , partitionMap.keySet().toSeq.sortWith((s, t) = > s < t));
            result.put(topic, Sc.toList(partitionMap.keySet()));
        });
        return result;
    }

    public static Map<TopicAndPartition, ReassignedPartitionsContext> getPartitionsBeingReassigned(ZkClient zkClient) {
        // read the partitions and their new replica list;
        Optional<String> jsonPartitionMapOpt = readDataMaybeNull(zkClient, ReassignPartitionsPath).v1;
        if (jsonPartitionMapOpt.isPresent()) {
            Map<TopicAndPartition, List<Integer>> reassignedPartitions = parsePartitionReassignmentData(jsonPartitionMapOpt.get());
            return Sc.mapValue(reassignedPartitions, v -> new ReassignedPartitionsContext(v));
        } else {
            return Maps.newHashMap();
        }
    }

    // Parses without deduplicating keys so the the data can be checked before allowing reassignment to proceed;
    public static List<Tuple<TopicAndPartition, List<Integer>>> parsePartitionReassignmentDataWithoutDedup(String jsonData) {
        Map<String, Object> m = JSON.parseObject(jsonData, Map.class);
        if (m != null) {
            Object partitions = m.get("partitions");
            if (partitions != null) {
                List<Map<String, Object>> partitionsSeq = (List<Map<String, Object>>) partitions;
                Sc.map(partitionsSeq, p -> {
                    String topic = p.get("topic").toString();
                    Integer partition = (Integer) p.get("partition");
                    List<Integer> newReplicas = (List) p.get("replicas");
                    return Tuple.of(new TopicAndPartition(topic, partition), newReplicas);
                });
            }
        }
        return Collections.emptyList();
    }

    public static Map<TopicAndPartition, List<Integer>> parsePartitionReassignmentData(String jsonData) {
        return Sc.toMap(parsePartitionReassignmentDataWithoutDedup(jsonData));
    }

    public static List<String> parseTopicsData(String jsonData) {
        List<String> topics = Lists.newArrayList();
        Map<String, Object> m = JSON.parseObject(jsonData, Map.class);
        Object partitionsSeq = m.get("topics");
        if (partitionsSeq != null) {
            List<Map<String, Object>> mapPartitionSeq = (List) partitionsSeq;
            mapPartitionSeq.forEach(p -> {
                String topic = p.get("topic").toString();
                topics.add(topic);
            });
        }
        return topics;
    }

    public static String getPartitionReassignmentZkData(Map<TopicAndPartition, List<Integer>> partitionsToBeReassigned) {
        return JSON.toJSONString(ImmutableMap.of("version", 1, "partitions",
                Sc.map(partitionsToBeReassigned,
                        e -> ImmutableMap.of("topic", e.getKey().topic, "partition", e.getKey().partition, "replicas", e.getValue()))));
    }

    public static void updatePartitionReassignmentData(ZkClient zkClient, Map<TopicAndPartition, List<Integer>> partitionsToBeReassigned) {
        String zkPath = ZkUtils.ReassignPartitionsPath;
        int len = partitionsToBeReassigned.size();
        if (len == 0) {
            deletePath(zkClient, zkPath);
            log.info(String.format("No more partitions need to be reassigned. Deleting zk path %s", zkPath));
        } else {
            String jsonData = getPartitionReassignmentZkData(partitionsToBeReassigned);
            try {
                updatePersistentPath(zkClient, zkPath, jsonData);
                log.info(String.format("Updated partition reassignment path with %s", jsonData));
            } catch (ZkNoNodeException nne) {
                ZkUtils.createPersistentPath(zkClient, zkPath, jsonData);
                log.debug(String.format("Created path %s with %s for partition reassignment", zkPath, jsonData));
            } catch (Throwable e2) {
                throw new AdminOperationException(e2.toString());
            }
        }
    }

    public static Set<TopicAndPartition> getPartitionsUndergoingPreferredReplicaElection(ZkClient zkClient) {
        // read the partitions and their new replica list;v
        Optional<String> jsonPartitionListOpt = readDataMaybeNull(zkClient, PreferredReplicaLeaderElectionPath).v1;
        if (jsonPartitionListOpt.isPresent()) {
            return PreferredReplicaLeaderElectionCommand.parsePreferredReplicaElectionData(jsonPartitionListOpt.get());
        }
        return Sets.newHashSet();
    }

    public static void deletePartition(ZkClient zkClient, Integer brokerId, String topic) {
        String brokerIdPath = BrokerIdsPath + "/" + brokerId;
        zkClient.delete(brokerIdPath);
        String brokerPartTopicPath = BrokerTopicsPath + "/" + topic + "/" + brokerId;
        zkClient.delete(brokerPartTopicPath);
    }

    public static List<String> getConsumersInGroup(ZkClient zkClient, String group) {
        ZKGroupDirs dirs = new ZKGroupDirs(group);
        return getChildren(zkClient, dirs.consumerRegistryDir());
    }

    public static Map<String, List<ConsumerThreadId>> getConsumersPerTopic(ZkClient zkClient, String group, Boolean excludeInternalTopics) {
        ZKGroupDirs dirs = new ZKGroupDirs(group);
        List<String> consumers = getChildrenParentMayNotExist(zkClient, dirs.consumerRegistryDir());
        Map<String, List<ConsumerThreadId>> consumersPerTopicMap = Maps.newHashMap();
        for (String consumer : consumers) {
            TopicCount topicCount = TopicCount.constructTopicCount(group, consumer, zkClient, excludeInternalTopics);
            topicCount.getConsumerThreadIdsPerTopic().forEach((topic, consumerThreadIdSet) -> {
                for (ConsumerThreadId consumerThreadId : consumerThreadIdSet) {
                    Object obj = consumersPerTopicMap.get(topic);
                    if (obj != null) {
                        consumersPerTopicMap.put(topic, Lists.newArrayList(consumerThreadId, (ConsumerThreadId) obj));
                    } else {
                        consumersPerTopicMap.put(topic, Lists.newArrayList((consumerThreadId)));
                    }
                }
            });
        }
        // TODO: 2017/11/1consumersPerTopicMap.put(topic, consumerList.sortWith((s, t) = > s < t));
        consumersPerTopicMap.forEach((topic, consumerList) -> consumersPerTopicMap.put(topic, consumerList));
        return consumersPerTopicMap;
    }

    /**
     * This API takes in a broker id, queries zookeeper for the broker metadata and returns the metadata for that broker
     * or throws an exception if the broker dies before the query to zookeeper finishes
     *
     * @param brokerId The broker id
     * @param zkClient The zookeeper client connection
     * @return An optional Broker object encapsulating the broker metadata
     */
    public static Optional<Broker> getBrokerInfo(ZkClient zkClient, Integer brokerId) {
        Optional<String> brokerInfoOpt = ZkUtils.readDataMaybeNull(zkClient, ZkUtils.BrokerIdsPath + "/" + brokerId).v1;
        if (brokerInfoOpt.isPresent()) {
            return Optional.of(Broker.createBroker(brokerId, brokerInfoOpt.get()));
        }
        return Optional.empty();
    }

    public static List<String> getAllTopics(ZkClient zkClient) {
        List<String> topics = ZkUtils.getChildrenParentMayNotExist(zkClient, BrokerTopicsPath);
        if (topics == null)
            return Collections.emptyList();
        else
            return topics;
    }

    public static Set<TopicAndPartition> getAllPartitions(ZkClient zkClient) {
        List<String> topics = ZkUtils.getChildrenParentMayNotExist(zkClient, BrokerTopicsPath);
        if (topics == null) {
            return Sets.newHashSet();
        } else {
            Set<TopicAndPartition> result = Sets.newHashSet();
            topics.forEach(topic ->
                    getChildren(zkClient, getTopicPartitionsPath(topic)).forEach(
                            s -> result.add(new TopicAndPartition(topic, Integer.parseInt(s)))));
            return result;
        }
    }
}





