package kafka.admin;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import kafka.api.PartitionMetadata;
import kafka.api.TopicMetadata;
import kafka.cluster.Broker;
import kafka.common.*;
import kafka.controller.KafkaController;
import kafka.func.Tuple;
import kafka.log.LogConfig;
import kafka.log.TopicAndPartition;
import kafka.utils.Logging;
import kafka.utils.Prediction;
import kafka.utils.Sc;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import java.util.*;

/**
 * @author zhoulf
 * @create 2017-10-23 28 10
 **/

public class AdminUtils  {
    public static Logging logging = Logging.getLogger(KafkaController.class.getName());
    public static final Random rand = new Random();
    public static final String TopicConfigChangeZnodePrefix = "config_change_";

    /**
     * There are 2 goals of replica assignment:
     * 1. Spread the replicas evenly among brokers.
     * 2. For partitions assigned to a particular broker, their other replicas are spread over the other brokers.
     * <p>
     * To achieve this goal, we:
     * 1. Assign the first replica of each partition by round-robin, starting from a random position in the broker list.
     * 2. Assign the remaining replicas of each partition with an increasing shift.
     * <p>
     * Here is an example of assigning
     * broker-0  broker-1  broker-2  broker-3  broker-4
     * p0        p1        p2        p3        p4       (1st replica)
     * p5        p6        p7        p8        p9       (1st replica)
     * p4        p0        p1        p2        p3       (2nd replica)
     * p8        p9        p5        p6        p7       (2nd replica)
     * p3        p4        p0        p1        p2       (3nd replica)
     * p7        p8        p9        p5        p6       (3nd replica)
     */
    public static Map<Integer, List<Integer>> assignReplicasToBrokers(List<Integer> brokerList, Integer nPartitions, Integer replicationFactor) {
        return assignReplicasToBrokers(brokerList, nPartitions, replicationFactor, -1, -1);
    }

    public static Map<Integer, List<Integer>> assignReplicasToBrokers(List<Integer> brokerList, Integer nPartitions, Integer replicationFactor, Integer fixedStartIndex, Integer startPartitionId) {
        if (nPartitions <= 0)
            throw new AdminOperationException("number of partitions must be larger than 0");
        if (replicationFactor <= 0)
            throw new AdminOperationException("replication factor must be larger than 0");
        if (replicationFactor > brokerList.size())
            throw new AdminOperationException("replication factor: " + replicationFactor +
                    " larger than available brokers: " + brokerList.size());
        HashMap<Integer, List<Integer>> ret = new HashMap<>();
        int startIndex = (fixedStartIndex >= 0) ? fixedStartIndex : rand.nextInt(brokerList.size());
        int currentPartitionId = (startPartitionId >= 0) ? startPartitionId : 0;

        int nextReplicaShift = (fixedStartIndex >= 0) ? fixedStartIndex : rand.nextInt(brokerList.size());
        for (int i = 0; i < nPartitions; i++) {
            if (currentPartitionId > 0 && (currentPartitionId % brokerList.size() == 0))
                nextReplicaShift += 1;
            int firstReplicaIndex = (currentPartitionId + startIndex) % brokerList.size();
            List replicaList = Lists.newArrayList(brokerList.get(firstReplicaIndex));
            for (int j = 0; j < replicationFactor - 1; j++)
                replicaList.add(brokerList.get(replicaIndex(firstReplicaIndex, nextReplicaShift, j, brokerList.size())));
            Collections.reverse(replicaList);
            ret.put(currentPartitionId, replicaList);
            currentPartitionId = currentPartitionId + 1;
        }
        return ret;
    }


    /**
     * Add partitions to existing topic with optional replica assignment
     *
     * @param zkClient             Zookeeper client
     * @param topic                Topic for adding partitions to
     * param numPartitions        Number of partitions to be set
     * param replicaAssignmentStr Manual replica assignment
     * param checkBrokerAvailable Ignore checking if assigned replica broker is available. Only used for testing
     * param config               Pre-existing properties that should be preserved
     */
    public static void addPartitions(ZkClient zkClient, String topic) {
        addPartitions(zkClient, topic, 1, "", true, new Properties());

    }

    public static void addPartitions(ZkClient zkClient, String topic, Integer numPartitions, String replicaAssignmentStr, Boolean checkBrokerAvailable, Properties config) {
        Map<TopicAndPartition, List<Integer>> existingPartitionsReplicaList = ZkUtils.getReplicaAssignmentForTopics(zkClient, Lists.newArrayList(topic));
        if (existingPartitionsReplicaList.size() == 0)
            throw new AdminOperationException(String.format("The topic %s does not exist", topic));

        List<Integer> existingReplicaList = Sc.head(existingPartitionsReplicaList).v2;
        int partitionsToAdd = numPartitions - existingPartitionsReplicaList.size();
        if (partitionsToAdd <= 0)
            throw new AdminOperationException("The number of partitions for a topic can only be increased");

        // create the new partition replication list;
        List<Integer> brokerList = ZkUtils.getSortedBrokerList(zkClient);
        Map<Integer, List<Integer>> newPartitionReplicaList = (replicaAssignmentStr == null || replicaAssignmentStr == "")
                ? AdminUtils.assignReplicasToBrokers(brokerList, partitionsToAdd, existingReplicaList.size(), Sc.head(existingReplicaList), existingPartitionsReplicaList.size())
                : getManualReplicaAssignment(replicaAssignmentStr, Sc.toSet(brokerList), existingPartitionsReplicaList.size(), checkBrokerAvailable);

        // check if manual assignment has the right replication factor;
        List<List<Integer>> unmatchedRepFactorList = Sc.filter(newPartitionReplicaList.values(), p -> (p.size() != existingReplicaList.size()));
        if (unmatchedRepFactorList.size() != 0)
            throw new AdminOperationException("The replication factor in manual replication assignment " +
                    " is not equal to the existing replication factor for the topic " + existingReplicaList.size());

        logging.info(String.format("Add partition list for %s is %s", topic, newPartitionReplicaList));
        Map<Integer, List<Integer>> partitionReplicaList = Sc.mapKey(existingPartitionsReplicaList, k -> k.partition);
        // add the new list;
        partitionReplicaList.putAll(newPartitionReplicaList);
        AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, partitionReplicaList, config, true);
    }

    public static Map<Integer, List<Integer>> getManualReplicaAssignment(String replicaAssignmentList, Set<Integer> availableBrokerList, Integer startPartitionId) {
        return getManualReplicaAssignment(replicaAssignmentList,availableBrokerList,startPartitionId,true);
    }

    public static Map<Integer, List<Integer>> getManualReplicaAssignment(String replicaAssignmentList, Set<Integer> availableBrokerList, Integer startPartitionId, Boolean checkBrokerAvailable) {
        List<String> partitionList = Arrays.asList(replicaAssignmentList.split(","));
        HashMap<Integer, List<Integer>> ret = new HashMap<Integer, List<Integer>>();
        Integer partitionId = startPartitionId;
        partitionList = Sc.takeRight(partitionList, partitionList.size() - partitionId);
        for (int i = 0; i < partitionList.size(); i++) {
            List<Integer> brokerList = Sc.map(partitionList.get(i).split(":"), s -> java.lang.Integer.parseInt(s.trim()));
            if (brokerList.size() <= 0)
                throw new AdminOperationException("replication factor must be larger than 0");
            if (brokerList.size() != Sc.toSet(brokerList).size())
                throw new AdminOperationException("duplicate brokers in replica assignment: " + brokerList);
            if (checkBrokerAvailable && !Sc.subsetOf(Sc.toSet(brokerList), availableBrokerList))
                throw new AdminOperationException("some specified brokers not available. specified brokers: " + brokerList.toString() +
                        "available broker:" + availableBrokerList.toString());
            ret.put(partitionId, brokerList);
            if (ret.get(partitionId).size() != ret.get(startPartitionId).size())
                throw new AdminOperationException("partition " + i + " has different replication factor: " + brokerList);
            partitionId = partitionId + 1;
        }
        return ret;
    }

    public static void deleteTopic(ZkClient zkClient, String topic) {
        ZkUtils.createPersistentPath(zkClient, ZkUtils.getDeleteTopicPath(topic));
    }

    public static Boolean topicExists(ZkClient zkClient, String topic) {
        return zkClient.exists(ZkUtils.getTopicPath(topic));
    }

    public static void createTopic(ZkClient zkClient,
                                   String topic,
                                   Integer partitions,
                                   Integer replicationFactor) {
        createTopic(zkClient, topic, partitions, replicationFactor, new Properties());
    }

    public static void createTopic(ZkClient zkClient,
                                   String topic,
                                   Integer partitions,
                                   Integer replicationFactor,
                                   Properties topicConfig) {
        List<Integer> brokerList = ZkUtils.getSortedBrokerList(zkClient);
        Map<Integer, List<Integer>> replicaAssignment = AdminUtils.assignReplicasToBrokers(brokerList, partitions, replicationFactor);
        AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, replicaAssignment, topicConfig, false);
    }

    public static void createOrUpdateTopicPartitionAssignmentPathInZK(ZkClient zkClient,
                                                                      String topic,
                                                                      Map<Integer, List<Integer>> partitionReplicaAssignment) {
        createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, partitionReplicaAssignment, new Properties(), false);
    }

    public static void createOrUpdateTopicPartitionAssignmentPathInZK(ZkClient zkClient,
                                                                      String topic,
                                                                      Map<Integer, List<Integer>> partitionReplicaAssignment,
                                                                      Properties config,
                                                                      Boolean update) {
        // validate arguments;
        Topic.validate(topic);
        LogConfig.validate(config);
        Prediction.require(Sc.toSet(Sc.map(partitionReplicaAssignment.values(), p -> p.size())).size() == 1, "All partitions should have the same number of replicas.");

        String topicPath = ZkUtils.getTopicPath(topic);
        if (!update && zkClient.exists(topicPath))
            throw new TopicExistsException(String.format("Topic \"%s\" already exists.", topic));
        partitionReplicaAssignment.values().forEach(reps -> Prediction.require(reps.size() == Sc.toSet(reps).size(), "Duplicate replica assignment found: " + partitionReplicaAssignment));

        // write out the config if there is any, this isn't transactional with the partition assignments;
        writeTopicConfig(zkClient, topic, config);

        // create the partition assignment;
        writeTopicPartitionAssignment(zkClient, topic, partitionReplicaAssignment, update);
    }

    private static void writeTopicPartitionAssignment(ZkClient zkClient, String topic, Map<Integer, List<Integer>> replicaAssignment, Boolean update) {
        try {
            String zkPath = ZkUtils.getTopicPath(topic);
            String jsonPartitionData = ZkUtils.replicaAssignmentZkData(Sc.toMap(Sc.map(replicaAssignment, (k, v) -> Tuple.of(k.toString(), v))));

            if (!update) {
                logging.info("Topic creation " + jsonPartitionData.toString());
                ZkUtils.createPersistentPath(zkClient, zkPath, jsonPartitionData);
            } else {
                logging.info("Topic update " + jsonPartitionData.toString());
                ZkUtils.updatePersistentPath(zkClient, zkPath, jsonPartitionData);
            }
            logging.debug(String.format("Updated path %s with %s for replica assignment", zkPath, jsonPartitionData));
        } catch (ZkNodeExistsException e) {
            throw new TopicExistsException(String.format("topic %s already exists", topic));
        } catch (Throwable e2) {
            throw new AdminOperationException(e2.toString());
        }
    }

    /**
     * Update the config for an existing topic and create a change notification so the change will propagate to other brokers
     *
     * param The zkClient ZkClient handle used to write the new config to zookeeper
     * param The topic topic for which configs are being changed
     * param The configs final set of configs that will be applied to the topic. If any new configs need to be added or
     *            existing configs need to be deleted, it should be done prior to invoking this API
     */
    public void changeTopicConfig(ZkClient zkClient, String topic, Properties configs) {
        if (!topicExists(zkClient, topic))
            throw new AdminOperationException(String.format("Topic \"%s\" does not exist.", topic));

        // remove the topic  @Overrides
        LogConfig.validate(configs);

        // write the new config--may not exist if there were previously no  @Overrides
        writeTopicConfig(zkClient, topic, configs);

        // create the change notification;
        zkClient.createPersistentSequential(ZkUtils.TopicConfigChangesPath + "/" + TopicConfigChangeZnodePrefix, JSON.toJSON(topic));
    }

    /**
     * Write out the topic config to zk, if there is any
     */
    private static void writeTopicConfig(ZkClient zkClient, String topic, Properties config) {
        Map<String, String> configMap = Sc.toMap(config);
        Map map = ImmutableMap.of("version", 1, "config", configMap);
        ZkUtils.updatePersistentPath(zkClient, ZkUtils.getTopicConfigPath(topic), JSON.toJSONString(map));
    }

    /**
     * Read the topic config (if any) from zk
     */
    public static Properties fetchTopicConfig(ZkClient zkClient, String topic) {
        String str = zkClient.readData(ZkUtils.getTopicConfigPath(topic), true);
        Properties props = new Properties();
        if (str != null) {
            Map map = JSON.parseObject(str, Map.class);
            if (map != null) {
                Prediction.require(map.get("version").equals("1"));
                Object obj = map.get("config");
                if (obj != null) {
                    Map<String, String> config = (Map<String, String>) obj;
                    config.forEach((k, v) -> props.setProperty(k, v));
                } else {
                    throw new IllegalArgumentException("Invalid topic config: " + str);
                }
            } else {
                throw new IllegalArgumentException("Unexpected value in config: " + str);
            }
        }
        return props;
    }

    public Map<String, Properties> fetchAllTopicConfigs(ZkClient zkClient) {
        return Sc.mapToMap(ZkUtils.getAllTopics(zkClient), topic -> Tuple.of(topic, fetchTopicConfig(zkClient, topic)));
    }


    public TopicMetadata fetchTopicMetadataFromZk(String topic, ZkClient zkClient) {
        return fetchTopicMetadataFromZk(topic, zkClient, new HashMap<Integer, Broker>());
    }

    public Set<TopicMetadata> fetchTopicMetadataFromZk(Set<String> topics, ZkClient zkClient) {
        HashMap<Integer, Broker> cachedBrokerInfo = new HashMap<Integer, Broker>();
        return Sc.map(topics, topic -> fetchTopicMetadataFromZk(topic, zkClient, cachedBrokerInfo));
    }

    private TopicMetadata fetchTopicMetadataFromZk(String topic, ZkClient zkClient, HashMap<Integer, Broker> cachedBrokerInfo) {
        if (ZkUtils.pathExists(zkClient, ZkUtils.getTopicPath(topic))) {
            Map<Integer, List<Integer>> topicPartitionAssignment = ZkUtils.getPartitionAssignmentForTopics(zkClient, Lists.newArrayList(topic)).get(topic);
            List<Tuple<Integer, List<Integer>>> sortedPartitions = Sc.sortWith(Sc.toList(topicPartitionAssignment), (m1, m2) -> m1.v1 < m2.v1);
            List<PartitionMetadata> partitionMetadata = Sc.map(sortedPartitions, partitionMap -> {
                Integer partition = partitionMap.v1;
                List<Integer> replicas = partitionMap.v2;
                List<Integer> inSyncReplicas = ZkUtils.getInSyncReplicasForPartition(zkClient, topic, partition);
                Optional<Integer> leader = ZkUtils.getLeaderForPartition(zkClient, topic, partition);
                logging.debug("replicas = " + replicas + ", in sync replicas = " + inSyncReplicas + ", leader = " + leader);

                Optional<Broker> leaderInfo = Optional.empty();
                List<Broker> replicaInfo = null;
                List<Broker> isrInfo = null;
                try {
                    if (leader.isPresent()) {
                        Integer l = leader.get();
                        try {
                            leaderInfo = Optional.of(Sc.head(getBrokerInfoFromCache(zkClient, cachedBrokerInfo, Lists.newArrayList(l))));
                        } catch (Throwable e) {
                            throw new LeaderNotAvailableException(String.format("Leader not available for partition <%s,%d>", topic, partition), e);
                        }
                    } else {
                        throw new LeaderNotAvailableException("No leader exists for partition " + partition);
                    }
                    try {
                        replicaInfo = getBrokerInfoFromCache(zkClient, cachedBrokerInfo, replicas);
                        isrInfo = getBrokerInfoFromCache(zkClient, cachedBrokerInfo, inSyncReplicas);
                    } catch (Throwable e) {
                        throw new ReplicaNotAvailableException(e);
                    }
                    if (replicaInfo.size() < replicas.size()) {
                        final List<Broker> replicaInfo2 = replicaInfo;
                        throw new ReplicaNotAvailableException("Replica information not available for following brokers: " +
                                Sc.filterNot(replicas, replica -> Sc.map(replicaInfo2, r -> r.id).contains(replica)));
                    }
                    if (isrInfo.size() < inSyncReplicas.size()) {
                        final List<Broker> isrInfo2 = isrInfo;
                        throw new ReplicaNotAvailableException("In Sync Replica information not available for following brokers: " +
                                Sc.filterNot(inSyncReplicas, isr -> Sc.map(isrInfo2, r -> r.id).contains(isr)));
                    }
                    return new PartitionMetadata(partition, leaderInfo.get(), replicaInfo, isrInfo, ErrorMapping.NoError);
                } catch (Throwable e) {
                    logging.debug(String.format("Error while fetching metadata for partition <%s,%d>", topic, partition), e);
                    return new PartitionMetadata(partition, leaderInfo.get(), replicaInfo, isrInfo,
                            ErrorMapping.codeFor(e.getClass()));
                }
            });
            return new TopicMetadata(topic, partitionMetadata);
        } else {
            // topic doesn't exist, send appropriate error code;
           return  new TopicMetadata(topic, Lists.newArrayList(), ErrorMapping.UnknownTopicOrPartitionCode);
        }
    }

    private List<Broker> getBrokerInfoFromCache(ZkClient zkClient, final Map<Integer, Broker> cachedBrokerInfo, List<Integer> brokerIds) {
        List<Integer> failedBrokerIds = Lists.newArrayList();
        List<Optional<Broker>> brokerMetadata = Sc.map(brokerIds, id -> {
            Broker brokerInfo = cachedBrokerInfo.get(id);
            if (brokerInfo != null) {
                return Optional.of(brokerInfo);
            } else {
                Optional<Broker> opt = ZkUtils.getBrokerInfo(zkClient, id);
                if (opt.isPresent()) {
                    cachedBrokerInfo.put(id, brokerInfo);
                    return Optional.of(brokerInfo);
                } else {
                    failedBrokerIds.add(id);
                    return Optional.empty();
                }
            }
        });
        return Sc.map(Sc.filter(brokerMetadata, b -> b.isPresent()), b -> b.get());
    }

    private static Integer replicaIndex(Integer firstReplicaIndex, Integer secondReplicaShift, Integer replicaIndex, Integer nBrokers) {
        Integer shift = 1 + (secondReplicaShift + replicaIndex) % (nBrokers - 1);
        return (firstReplicaIndex + shift) % nBrokers;
    }
}
