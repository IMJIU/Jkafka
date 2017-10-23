package kafka.admin;

import kafka.api.TopicMetadata;
import kafka.cluster.Broker;
import kafka.utils.Logging;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;

import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

/**
 * @author zhoulf
 * @create 2017-10-23 28 10
 **/

public class AdminUtils extends Logging {
   public static final Random rand = new Random();
    public static final     String TopicConfigChangeZnodePrefix = "config_change_";

        /**
         * There are 2 goals of replica assignment:
         * 1. Spread the replicas evenly among brokers.
         * 2. For partitions assigned to a particular broker, their other replicas are spread over the other brokers.
         *
         * To achieve this goal, we:
         * 1. Assign the first replica of each partition by round-robin, starting from a random position in the broker list.
         * 2. Assign the remaining replicas of each partition with an increasing shift.
         *
         * Here is an example of assigning
         * broker-0  broker-1  broker-2  broker-3  broker-4
         * p0        p1        p2        p3        p4       (1st replica)
         * p5        p6        p7        p8        p9       (1st replica)
         * p4        p0        p1        p2        p3       (2nd replica)
         * p8        p9        p5        p6        p7       (2nd replica)
         * p3        p4        p0        p1        p2       (3nd replica)
         * p7        p8        p9        p5        p6       (3nd replica)
         */
       public void assignReplicasToBrokers(Seq brokerList<Integer>,
        Int nPartitions,
        Int replicationFactor,
        Integer fixedStartIndex = -1,
        Integer startPartitionId = -1);
        : Map<Integer,> Seq<Int>> = {
        if (nPartitions <= 0)
        throw new AdminOperationException("number of partitions must be larger than 0");
        if (replicationFactor <= 0)
        throw new AdminOperationException("replication factor must be larger than 0");
        if (replicationFactor > brokerList.size)
        throw new AdminOperationException("replication factor: " + replicationFactor +
        " larger than available brokers: " + brokerList.size);
        val ret = new mutable.HashMap<Integer,> List<Int>>();
        val startIndex = if (fixedStartIndex >= 0) fixedStartIndex else rand.nextInt(brokerList.size)
        var currentPartitionId = if (startPartitionId >= 0) startPartitionId else 0;

        var nextReplicaShift = if (fixedStartIndex >= 0) fixedStartIndex else rand.nextInt(brokerList.size)
        for (i <- 0 until nPartitions) {
        if (currentPartitionId > 0 && (currentPartitionId % brokerList.size == 0))
        nextReplicaShift += 1;
        val firstReplicaIndex = (currentPartitionId + startIndex) % brokerList.size;
        var replicaList = List(brokerList(firstReplicaIndex));
        for (j <- 0 until replicationFactor - 1)
        replicaList ::= brokerList(replicaIndex(firstReplicaIndex, nextReplicaShift, j, brokerList.size))
        ret.put(currentPartitionId, replicaList.reverse);
        currentPartitionId = currentPartitionId + 1;
        }
        ret.toMap;
        }


        /**
         * Add partitions to existing topic with optional replica assignment
         *
         * @param zkClient Zookeeper client
         * @param topic Topic for adding partitions to
         * @param numPartitions Number of partitions to be set
         * @param replicaAssignmentStr Manual replica assignment
         * @param checkBrokerAvailable Ignore checking if assigned replica broker is available. Only used for testing
         * @param config Pre-existing properties that should be preserved
         */
       public void addPartitions(ZkClient zkClient,
        String topic,
        Integer numPartitions = 1,
        String replicaAssignmentStr = "",
        Boolean checkBrokerAvailable = true,
        Properties config = new Properties) {
        val existingPartitionsReplicaList = ZkUtils.getReplicaAssignmentForTopics(zkClient, List(topic));
        if (existingPartitionsReplicaList.size == 0)
        throw new AdminOperationException(String.format("The topic %s does not exist",topic))

        val existingReplicaList = existingPartitionsReplicaList.head._2;
        val partitionsToAdd = numPartitions - existingPartitionsReplicaList.size;
        if (partitionsToAdd <= 0)
        throw new AdminOperationException("The number of partitions for a topic can only be increased")

        // create the new partition replication list;
        val brokerList = ZkUtils.getSortedBrokerList(zkClient);
        val newPartitionReplicaList = if (replicaAssignmentStr == null || replicaAssignmentStr == "")
        AdminUtils.assignReplicasToBrokers(brokerList, partitionsToAdd, existingReplicaList.size, existingReplicaList.head, existingPartitionsReplicaList.size);
        else
        getManualReplicaAssignment(replicaAssignmentStr, brokerList.toSet, existingPartitionsReplicaList.size, checkBrokerAvailable);

        // check if manual assignment has the right replication factor;
        val unmatchedRepFactorList = newPartitionReplicaList.values.filter(p => (p.size != existingReplicaList.size));
        if (unmatchedRepFactorList.size != 0)
        throw new AdminOperationException("The replication factor in manual replication assignment " +
        " is not equal to the existing replication factor for the topic " + existingReplicaList.size)

        info(String.format("Add partition list for %s is %s",topic, newPartitionReplicaList))
        val partitionReplicaList = existingPartitionsReplicaList.map(p => p._1.partition -> p._2);
        // add the new list;
        partitionReplicaList ++= newPartitionReplicaList;
        AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, partitionReplicaList, config, true);
        }

       public void getManualReplicaAssignment(String replicaAssignmentList, Set availableBrokerList<Int], Int startPartitionId, Boolean checkBrokerAvailable = true): Map<Int, List[Int>> = {
        var partitionList = replicaAssignmentList.split(",");
        val ret = new mutable.HashMap<Integer,> List<Int>>();
        var partitionId = startPartitionId;
        partitionList = partitionList.takeRight(partitionList.size - partitionId);
        for (i <- 0 until partitionList.size) {
        val brokerList = partitionList(i).split(":").map(s => s.trim().toInt);
        if (brokerList.size <= 0)
        throw new AdminOperationException("replication factor must be larger than 0");
        if (brokerList.size != brokerList.toSet.size)
        throw new AdminOperationException("duplicate brokers in replica assignment: " + brokerList);
        if (checkBrokerAvailable && !brokerList.toSet.subsetOf(availableBrokerList))
        throw new AdminOperationException("some specified brokers not available. specified brokers: " + brokerList.toString +
        "available broker:" + availableBrokerList.toString);
        ret.put(partitionId, brokerList.toList);
        if (ret(partitionId).size != ret(startPartitionId).size)
        throw new AdminOperationException("partition " + i + " has different replication factor: " + brokerList)
        partitionId = partitionId + 1;
        }
        ret.toMap;
        }

       public void deleteTopic(ZkClient zkClient, String topic) {
        ZkUtils.createPersistentPath(zkClient, ZkUtils.getDeleteTopicPath(topic));
        }

       public Boolean  void topicExists(ZkClient zkClient, String topic);
        zkClient.exists(ZkUtils.getTopicPath(topic));

       public void createTopic(ZkClient zkClient,
        String topic,
        Int partitions,
        Int replicationFactor,
        Properties topicConfig = new Properties) {
        val brokerList = ZkUtils.getSortedBrokerList(zkClient);
        val replicaAssignment = AdminUtils.assignReplicasToBrokers(brokerList, partitions, replicationFactor);
        AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, replicaAssignment, topicConfig);
        }

       public void createOrUpdateTopicPartitionAssignmentPathInZK(ZkClient zkClient,
        String topic,
        Map partitionReplicaAssignment<Integer,> Seq<Int>>,
        Properties config = new Properties,
        Boolean update = false) {
        // validate arguments;
        Topic.validate(topic);
        LogConfig.validate(config);
        require(partitionReplicaAssignment.values.map(_.size).toSet.size == 1, "All partitions should have the same number of replicas.");

        val topicPath = ZkUtils.getTopicPath(topic);
        if(!update && zkClient.exists(topicPath))
        throw new TopicExistsException(String.format("Topic \"%s\" already exists.",topic))
        partitionReplicaAssignment.values.foreach(reps => require(reps.size == reps.toSet.size, "Duplicate replica assignment found: "  + partitionReplicaAssignment))

        // write out the config if there is any, this isn't transactional with the partition assignments;
        writeTopicConfig(zkClient, topic, config);

        // create the partition assignment;
        writeTopicPartitionAssignment(zkClient, topic, partitionReplicaAssignment, update);
        }

privatepublic void writeTopicPartitionAssignment(ZkClient zkClient, String topic, Map replicaAssignment<Integer,> Seq<Int>>, Boolean update) {
        try {
        val zkPath = ZkUtils.getTopicPath(topic);
        val jsonPartitionData = ZkUtils.replicaAssignmentZkData(replicaAssignment.map(e => (e._1.toString -> e._2)));

        if (!update) {
        info("Topic creation " + jsonPartitionData.toString);
        ZkUtils.createPersistentPath(zkClient, zkPath, jsonPartitionData);
        } else {
        info("Topic update " + jsonPartitionData.toString);
        ZkUtils.updatePersistentPath(zkClient, zkPath, jsonPartitionData);
        }
        debug(String.format("Updated path %s with %s for replica assignment",zkPath, jsonPartitionData))
        } catch {
        case ZkNodeExistsException e => throw new TopicExistsException(String.format("topic %s already exists",topic))
        case Throwable e2 => throw new AdminOperationException(e2.toString);
        }
        }

        /**
         * Update the config for an existing topic and create a change notification so the change will propagate to other brokers
         * @param The zkClient ZkClient handle used to write the new config to zookeeper
         * @param The topic topic for which configs are being changed
         * @param The configs final set of configs that will be applied to the topic. If any new configs need to be added or
         *                 existing configs need to be deleted, it should be done prior to invoking this API
         *
         */
       public void changeTopicConfig(ZkClient zkClient, String topic, Properties configs) {
        if(!topicExists(zkClient, topic))
        throw new AdminOperationException(String.format("Topic \"%s\" does not exist.",topic))

        // remove the topic  @Overrides
        LogConfig.validate(configs);

        // write the new config--may not exist if there were previously no  @Overrides
        writeTopicConfig(zkClient, topic, configs);

        // create the change notification;
        zkClient.createPersistentSequential(ZkUtils.TopicConfigChangesPath + "/" + TopicConfigChangeZnodePrefix, Json.encode(topic));
        }

/**
 * Write out the topic config to zk, if there is any
 */
privatepublic void writeTopicConfig(ZkClient zkClient, String topic, Properties config) {
        val mutable configMap.Map<String, String> = {
        import JavaConversions._;
        config;
        }
        val map = Map("version" -> 1, "config" -> configMap);
        ZkUtils.updatePersistentPath(zkClient, ZkUtils.getTopicConfigPath(topic), Json.encode(map));
        }

        /**
         * Read the topic config (if any) from zk
         */
       public static Properties   fetchTopicConfig(ZkClient zkClient, String topic) {
         String str = zkClient.readData(ZkUtils.getTopicConfigPath(topic), true);
           Properties props = new Properties();
        if(str != null) {

        Json.parseFull(str) match {
        case None => // there are no config  @Overrides
        case Some(Map map<String, _>) =>
        require(map("version") == 1);
        map.get("config") match {
        case Some(Map config<String, String>) =>
        for((k,v) <- config)
        props.setProperty(k, v);
        case _ => throw new IllegalArgumentException("Invalid topic config: " + str);
        }

        case o => throw new IllegalArgumentException("Unexpected value in config: "  + str);
        }
        }
        return props;
        }

       public void fetchAllTopicConfigs(ZkClient zkClient): Map<String, Properties> =
        ZkUtils.getAllTopics(zkClient).map(topic => (topic, fetchTopicConfig(zkClient, topic))).toMap;

       public TopicMetadata  void fetchTopicMetadataFromZk(String topic, ZkClient zkClient);
        fetchTopicMetadataFromZk(topic, zkClient, new mutable.HashMap<Integer,> Broker>);

       public Set<TopicMetadata> fetchTopicMetadataFromZk(Set<String> topics, ZkClient zkClient) {
        val cachedBrokerInfo = new mutable.HashMap<Integer,> Broker>();
        topics.map(topic => fetchTopicMetadataFromZk(topic, zkClient, cachedBrokerInfo));
        }

private TopicMetadata fetchTopicMetadataFromZk(String topic, ZkClient zkClient,HashMap<Integer, Broker> cachedBrokerInfo) {
        if(ZkUtils.pathExists(zkClient, ZkUtils.getTopicPath(topic))) {
        val topicPartitionAssignment = ZkUtils.getPartitionAssignmentForTopics(zkClient, List(topic)).get(topic).get;
        val sortedPartitions = topicPartitionAssignment.toList.sortWith((m1, m2) => m1._1 < m2._1);
        val partitionMetadata = sortedPartitions.map { partitionMap =>
        val partition = partitionMap._1;
        val replicas = partitionMap._2;
        val inSyncReplicas = ZkUtils.getInSyncReplicasForPartition(zkClient, topic, partition);
        val leader = ZkUtils.getLeaderForPartition(zkClient, topic, partition);
        debug("replicas = " + replicas + ", in sync replicas = " + inSyncReplicas + ", leader = " + leader);

        var Option leaderInfo<Broker> = None;
        var Seq replicaInfo<Broker> = Nil;
        var Seq isrInfo<Broker> = Nil;
        try {
        leaderInfo = leader match {
        case Some(l) =>
        try {
        Some(getBrokerInfoFromCache(zkClient, cachedBrokerInfo, List(l)).head);
        } catch {
        case Throwable e => throw new LeaderNotAvailableException(String.format("Leader not available for partition <%s,%d>",topic, partition), e)
        }
        case None => throw new LeaderNotAvailableException("No leader exists for partition " + partition)
        }
        try {
        replicaInfo = getBrokerInfoFromCache(zkClient, cachedBrokerInfo, replicas.map(id => id.toInt));
        isrInfo = getBrokerInfoFromCache(zkClient, cachedBrokerInfo, inSyncReplicas);
        } catch {
        case Throwable e => throw new ReplicaNotAvailableException(e);
        }
        if(replicaInfo.size < replicas.size)
        throw new ReplicaNotAvailableException("Replica information not available for following brokers: " +
        replicas.filterNot(replicaInfo.map(_.id).contains(_)).mkString(","));
        if(isrInfo.size < inSyncReplicas.size)
        throw new ReplicaNotAvailableException("In Sync Replica information not available for following brokers: " +
        inSyncReplicas.filterNot(isrInfo.map(_.id).contains(_)).mkString(","));
        new PartitionMetadata(partition, leaderInfo, replicaInfo, isrInfo, ErrorMapping.NoError);
        } catch {
        case Throwable e =>
        debug(String.format("Error while fetching metadata for partition <%s,%d>",topic, partition), e)
        new PartitionMetadata(partition, leaderInfo, replicaInfo, isrInfo,
        ErrorMapping.codeFor(e.getClass.asInstanceOf<Class<Throwable>>));
        }
        }
        new TopicMetadata(topic, partitionMetadata);
        } else {
        // topic doesn't exist, send appropriate error code;
        new TopicMetadata(topic, Seq.empty<PartitionMetadata>, ErrorMapping.UnknownTopicOrPartitionCode);
        }
        }

privatepublic void getBrokerInfoFromCache(ZkClient zkClient,
        scala cachedBrokerInfo.collection.mutable.Map<Integer,> Broker>,
        Seq brokerIds<Int]): Seq[Broker> = {
        var ListBuffer failedBrokerIds<Integer> = new ListBuffer();
        val brokerMetadata = brokerIds.map { id =>
        val optionalBrokerInfo = cachedBrokerInfo.get(id);
        optionalBrokerInfo match {
        case Some(brokerInfo) => Some(brokerInfo) // return broker info from the cache;
        case None => // fetch it from zookeeper;
        ZkUtils.getBrokerInfo(zkClient, id) match {
        case Some(brokerInfo) =>
        cachedBrokerInfo += (id -> brokerInfo);
        Some(brokerInfo);
        case None =>
        failedBrokerIds += id;
        None;
        }
        }
        }
        brokerMetadata.filter(_.isDefined).map(_.get);
        }

privatepublic Integer  void replicaIndex(Int firstReplicaIndex, Int secondReplicaShift, Int replicaIndex, Int nBrokers) {
        val shift = 1 + (secondReplicaShift + replicaIndex) % (nBrokers - 1)
        (firstReplicaIndex + shift) % nBrokers;
        }
        }
