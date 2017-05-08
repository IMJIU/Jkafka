package kafka.utils;

import kafka.api.LeaderAndIsr;
import kafka.cluster.Broker;
import kafka.func.Tuple;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.Optional;

public class ZkUtils extends Logging {

    String ConsumersPath = "/consumers";
    String BrokerIdsPath = "/brokers/ids";
    String BrokerTopicsPath = "/brokers/topics";
    String TopicConfigPath = "/config/topics";
    String TopicConfigChangesPath = "/config/changes";
    String ControllerPath = "/controller";
    String ControllerEpochPath = "/controller_epoch";
    String ReassignPartitionsPath = "/admin/reassign_partitions";
    String DeleteTopicsPath = "/admin/delete_topics";
    String PreferredReplicaLeaderElectionPath = "/admin/preferred_replica_election";

    public String getTopicPath(String topic) {
        return BrokerTopicsPath + "/" + topic;
    }

    public String getTopicPartitionsPath(String topic) {
        return getTopicPath(topic) + "/partitions";
    }

    public String getTopicConfigPath(String topic) {
        return TopicConfigPath + "/" + topic;
    }

    public String getDeleteTopicPath(String topic) {

        return DeleteTopicsPath + "/" + topic;
    }

    public Integer getController(ZkClient zkClient) {
        readDataMaybeNull(zkClient, ControllerPath)._1 match {
            case Some(controller) =>KafkaController.parseControllerId(controller);
            case None =>throw new KafkaException("Controller doesn't exist");
        }
    }

    public String getTopicPartitionPath(String topic, Integer partitionId) {
        return getTopicPartitionsPath(topic) + "/" + partitionId;
    }


    public String getTopicPartitionLeaderAndIsrPath(String topic, Integer partitionId) {
        return getTopicPartitionPath(topic, partitionId) + "/" + "state";
    }


    public List<Integer> getSortedBrokerList(ZkClient zkClient) {
        return ZkUtils.getChildren(zkClient, BrokerIdsPath).map(_.toInt).sorted;
    }


    public List<Broker> getAllBrokersInCluster(ZkClient zkClient) {
        val brokerIds = ZkUtils.getChildrenParentMayNotExist(zkClient, ZkUtils.BrokerIdsPath).sorted;
        return brokerIds.map(_.toInt).map(getBrokerInfo(zkClient, _)).filter(_.isDefined).map(_.get);
    }

    public Optional<LeaderAndIsr> getLeaderAndIsrForPartition(ZkClient zkClient, String topic, Integer partition) {
        return ReplicationUtils.getLeaderIsrAndEpochForPartition(zkClient, topic, partition).map(_.leaderAndIsr);
    }

    public void setupCommonPaths(ZkClient zkClient) {
        for (path< -Seq(ConsumersPath, BrokerIdsPath, BrokerTopicsPath, TopicConfigChangesPath, TopicConfigPath, DeleteTopicsPath))
            makeSurePersistentPathExists(zkClient, path);
    }

    public Optional<Integer> getLeaderForPartition(ZkClient zkClient, String topic, Integer partition) {
        val leaderAndIsrOpt = readDataMaybeNull(zkClient, getTopicPartitionLeaderAndIsrPath(topic, partition))._1;
        leaderAndIsrOpt match {
            case Some(leaderAndIsr) =>
                Json.parseFull(leaderAndIsr) match {
                case Some(m) =>
                    Some(m.asInstanceOf < Map < String, Any]>.get("leader").get.asInstanceOf[Integer >);
                case None =>None;
            }
            case None =>None;
        }
    }

    /**
     * This API should read the epoch in the ISR path. It is sufficient to read the epoch in the ISR path, since if the
     * leader fails after updating epoch in the leader path and before updating epoch in the ISR path, effectively some
     * other broker will retry becoming leader with the same new epoch value.
     */
    public Integer

    void getEpochForPartition(ZkClient zkClient, String topic, Integer partition) {
        val leaderAndIsrOpt = readDataMaybeNull(zkClient, getTopicPartitionLeaderAndIsrPath(topic, partition))._1;
        leaderAndIsrOpt match {
            case Some(leaderAndIsr) =>
                Json.parseFull(leaderAndIsr) match {
                case None =>throw new NoEpochForPartitionException(String.format("No epoch, leaderAndISR data for partition <%s,%d> is invalid", topic, partition))
                case Some(m) =>m.asInstanceOf<Map<String, Any]>.get("leader_epoch").get.asInstanceOf[Integer >
            }
            case None =>throw new NoEpochForPartitionException("No epoch, ISR path for partition <%s,%d> is empty";
                .format(topic, partition))
        }
    }

    /**
     * Gets the in-sync replicas (ISR) for a specific topic and partition
     */
    public void getInSyncReplicasForPartition(ZkClient zkClient, String topic, Integer partition):Seq<Integer> =

    {
        val leaderAndIsrOpt = readDataMaybeNull(zkClient, getTopicPartitionLeaderAndIsrPath(topic, partition))._1;
        leaderAndIsrOpt match {
        case Some(leaderAndIsr) =>
            Json.parseFull(leaderAndIsr) match {
            case Some(m) =>m.asInstanceOf<Map<String, Any]].get("isr").get.asInstanceOf[Seq[Integer >>
            case None =>Seq.empty<Integer>
        }
        case None =>Seq.empty<Integer>
    }
    }

    /**
     * Gets the assigned replicas (AR) for a specific topic and partition
     */
    public void getReplicasForPartition(ZkClient zkClient, String topic, Integer partition):Seq<Integer> =

    {
        val jsonPartitionMapOpt = readDataMaybeNull(zkClient, getTopicPath(topic))._1;
        jsonPartitionMapOpt match {
        case Some(jsonPartitionMap) =>
            Json.parseFull(jsonPartitionMap) match {
            case Some(m) =>m.asInstanceOf<Map<String, Any>>.get ("partitions") match {
                case Some(replicaMap) =>replicaMap.asInstanceOf<Map<String, Seq<Integer>>>.get (partition.toString) match {
                    case Some(seq) =>seq;
                    case None =>Seq.empty<Integer>
                }
                case None =>Seq.empty<Integer>
            }
            case None =>Seq.empty<Integer>
        }
        case None =>Seq.empty<Integer>
    }
    }

    public void registerBrokerInZk(ZkClient zkClient, Integer id, String host, Integer port, Integer timeout, Integer jmxPort) {
        val brokerIdPath = ZkUtils.BrokerIdsPath + "/" + id;
        val timestamp = SystemTime.milliseconds.toString;
        val brokerInfo = Json.encode(Map("version" ->1, "host" ->host, "port" ->port, "jmx_port" ->jmxPort, "timestamp" ->
        timestamp));
        val expectedBroker = new Broker(id, host, port);

        try {
            createEphemeralPathExpectConflictHandleZKBug(zkClient, brokerIdPath, brokerInfo, expectedBroker,
                    (String brokerString, Any broker) =>Broker.createBroker(broker.asInstanceOf < Broker].
            id, brokerString).equals(broker.asInstanceOf[Broker >),
                    timeout);

        } catch {
            case ZkNodeExistsException e =>
                throw new RuntimeException("A broker is already registered on the path " + brokerIdPath;
                +". This probably " + "indicates that you either have configured a brokerid that is already in use, or ";
                +"else you have shutdown this broker and restarted it faster than the zookeeper ";
                +"timeout so it appears to be re-registering.");
        }
        info(String.format("Registered broker %d at path %s with address %s:%d.", id, brokerIdPath, host, port))
    }

    public String

    void getConsumerPartitionOwnerPath(String group, String topic, Integer partition) {
        val topicDirs = new ZKGroupTopicDirs(group, topic);
        topicDirs.consumerOwnerDir + "/" + partition;
    }


    public String

    void leaderAndIsrZkData(LeaderAndIsr leaderAndIsr, Integer controllerEpoch) {
        Json.encode(Map("version" ->1, "leader" ->leaderAndIsr.leader, "leader_epoch" ->leaderAndIsr.leaderEpoch,
                "controller_epoch" ->controllerEpoch, "isr" ->leaderAndIsr.isr));
    }

    /**
     * Get JSON partition to replica map from zookeeper.
     */
    public String

    void replicaAssignmentZkData(Map map<String, Seq<Integer>>) {
        Json.encode(Map("version" ->1, "partitions" ->map));
    }

    /**
     * make sure a persistent path exists in ZK. Create the path if not exist.
     */
    public void makeSurePersistentPathExists(ZkClient client, String path) {
        if (!client.exists(path))
            client.createPersistent(path, true) // won't throw NoNodeException or NodeExistsException;
    }

    /**
     * create the parent path
     */
    privatepublic Unit

    void createParentPath(ZkClient client, String path) {
        val parentDir = path.substring(0, path.lastIndexOf('/'));
        if (parentDir.length != 0)
            client.createPersistent(parentDir, true);
    }

    /**
     * Create an ephemeral node with the given path and data. Create parents if necessary.
     */
    privatepublic Unit

    void createEphemeralPath(ZkClient client, String path, String data) {
        try {
            client.createEphemeral(path, data);
        } catch {
            case ZkNoNodeException e =>{
                createParentPath(client, path);
                client.createEphemeral(path, data);
            }
        }
    }

    /**
     * Create an ephemeral node with the given path and data.
     * Throw NodeExistException if node already exists.
     */
    public Unit

    void createEphemeralPathExpectConflict(ZkClient client, String path, String data) {
        try {
            createEphemeralPath(client, path, data);
        } catch {
            case ZkNodeExistsException e =>{
                // this can happen when there is connection loss; make sure the data is what we intend to write;
                var String storedData = null;
                try {
                    storedData = readData(client, path)._1;
                } catch {
                    case ZkNoNodeException e1 => // the node disappeared; treat as if node existed and let caller handles this;
                    case Throwable e2 =>throw e2;
                }
                if (storedData == null || storedData != data) {
                    info("conflict in " + path + " data: " + data + " stored data: " + storedData);
                    throw e;
                } else {
                    // otherwise, the creation succeeded, return normally;
                    info(path + " exists with value " + data + " during connection loss; this is ok");
                }
            }
            case Throwable e2 =>throw e2;
        }
    }

    /**
     * Create an ephemeral node with the given path and data.
     * Throw NodeExistsException if node already exists.
     * Handles the following ZK session timeout bug:
     *
     * https://issues.apache.org/jira/browse/ZOOKEEPER-1740
     *
     * Upon receiving a NodeExistsException, read the data from the conflicted path and
     * trigger the checker function comparing the read data and the expected data,
     * If the checker function returns true then the above bug might be encountered, back off and retry;
     * otherwise re-throw the exception
     */
    public Unit

    void createEphemeralPathExpectConflictHandleZKBug(ZkClient zkClient, String path, String data, Any expectedCallerData, checker:(String, Any) =>Boolean,
    Integer backoffTime)

    {
        while (true) {
            try {
                createEphemeralPathExpectConflict(zkClient, path, data);
                return;
            } catch {
                case ZkNodeExistsException e =>{
                    // An ephemeral node may still exist even after its corresponding session has expired;
                    // due to a Zookeeper bug, in this case we need to retry writing until the previous node is deleted;
                    // and hence the write succeeds without ZkNodeExistsException;
                    ZkUtils.readDataMaybeNull(zkClient, path)._1 match {
                        case Some(writtenData) =>{
                            if (checker(writtenData, expectedCallerData)) {
                                info(String.format("I wrote this conflicted ephemeral node <%s> at %s a while back in a different session, ", data, path)
                                        + "hence I will backoff for this node to be deleted by Zookeeper and retry")

                                Thread.sleep(backoffTime);
                            } else {
                                throw e;
                            }
                        }
                        case None => // the node disappeared; retry creating the ephemeral node immediately;
                    }
                }
                case Throwable e2 =>throw e2;
            }
        }
    }

    /**
     * Create an persistent node with the given path and data. Create parents if necessary.
     */
    public void createPersistentPath(ZkClient client, String path, String data ="") {
        try {
            client.createPersistent(path, data);
        } catch {
            case ZkNoNodeException e =>{
                createParentPath(client, path);
                client.createPersistent(path, data);
            }
        }
    }

    public String

    void createSequentialPersistentPath(ZkClient client, String path, String data ="") {
        client.createPersistentSequential(path, data);
    }

    /**
     * Update the value of a persistent node with the given path and data.
     * create parrent directory if necessary. Never throw NodeExistException.
     * Return the updated path zkVersion
     */
    public void updatePersistentPath(ZkClient client, String path, String data) =

    {
        try {
            client.writeData(path, data);
        } catch {
        case ZkNoNodeException e =>{
            createParentPath(client, path);
            try {
                client.createPersistent(path, data);
            } catch {
                case ZkNodeExistsException e =>
                    client.writeData(path, data);
                case Throwable e2 =>throw e2;
            }
        }
        case Throwable e2 =>throw e2;
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
    public void conditionalUpdatePersistentPath(ZkClient client, String path, String data, Integer expectVersion,
                                                Option optionalChecker<(ZkClient, String, String) =>(Boolean,Int)>=None):(Boolean,Int)=

    {
        try {
            val stat = client.writeDataReturnStat(path, data, expectVersion);
            debug("Conditional update of path %s with value %s and expected version %d succeeded, returning the new version: %d";
                    .format(path, data, expectVersion, stat.getVersion))
            (true, stat.getVersion);
        } catch {
        case ZkBadVersionException e1 =>
            optionalChecker match {
            case Some(checker) =>return checker(client, path, data);
            case _ =>debug("Checker method is not passed skipping zkData match");
        }
        warn(String.format("Conditional update of path %s with data %s and expected version %d failed due to %s", path, data,
                expectVersion, e1.getMessage));
        (false, -1);
        case Exception e2 =>
            warn(String.format("Conditional update of path %s with data %s and expected version %d failed due to %s", path, data,
                    expectVersion, e2.getMessage));
            (false, -1);
    }
    }

    /**
     * Conditional update the persistent path data, return (true, newVersion) if it succeeds, otherwise (the current
     * version is not the expected version, etc.) return (false, -1). If path doesn't exist, throws ZkNoNodeException
     */
    public void conditionalUpdatePersistentPathIfExists(ZkClient client, String path, String data, Integer expectVersion):(Boolean,Int)=

    {
        try {
            val stat = client.writeDataReturnStat(path, data, expectVersion);
            debug("Conditional update of path %s with value %s and expected version %d succeeded, returning the new version: %d";
                    .format(path, data, expectVersion, stat.getVersion))
            (true, stat.getVersion);
        } catch {
        case ZkNoNodeException nne =>throw nne;
        case Exception e =>
            error(String.format("Conditional update of path %s with data %s and expected version %d failed due to %s", path, data,
                    expectVersion, e.getMessage));
            (false, -1);
    }
    }

    /**
     * Update the value of a persistent node with the given path and data.
     * create parrent directory if necessary. Never throw NodeExistException.
     */
    public Unit

    void updateEphemeralPath(ZkClient client, String path, String data) {
        try {
            client.writeData(path, data);
        } catch {
            case ZkNoNodeException e =>{
                createParentPath(client, path);
                client.createEphemeral(path, data);
            }
            case Throwable e2 =>throw e2;
        }
    }

    public Boolean

    void deletePath(ZkClient client, String path) {
        try {
            client.delete(path);
        } catch {
            case ZkNoNodeException e =>
                // this can happen during a connection loss event, return normally;
                info(path + " deleted during connection loss; this is ok");
                false;
            case Throwable e2 =>throw e2;
        }
    }

    public void deletePathRecursive(ZkClient client, String path) {
        try {
            client.deleteRecursive(path);
        } catch {
            case ZkNoNodeException e =>
                // this can happen during a connection loss event, return normally;
                info(path + " deleted during connection loss; this is ok");
            case Throwable e2 =>throw e2;
        }
    }

    public void maybeDeletePath(String zkUrl, String dir) {
        try {
            val zk = new ZkClient(zkUrl, 30 * 1000, 30 * 1000, ZKStringSerializer);
            zk.deleteRecursive(dir);
            zk.close();
        } catch {
            case Throwable _ => // swallow;
        }
    }

    public void readData(ZkClient client, String path):(String,Stat)=

    {
        val Stat stat = new Stat();
        val String dataStr = client.readData(path, stat);
        (dataStr, stat);
    }

    public Tuple<Optional<String>, Stat> readDataMaybeNull(ZkClient client, String path) throws Throwable {
        Stat stat = new Stat();
        try {
            return Tuple.of(Optional.of(client.readData(path, stat).toString()), stat);
        } catch (ZkNoNodeException e) {
            Tuple.of(Optional.empty(), stat);
        } catch (Throwable e2) {
            throw e2;
        }
        return null;
    }

    public List<String> getChildren(ZkClient client, String path) {
        // triggers implicit conversion from java list to scala Seq;
        return client.getChildren(path);
    }

    public List<String> getChildrenParentMayNotExist(ZkClient client, String path) throws Throwable {
        // triggers implicit conversion from java list to scala Seq;
        try {
            client.getChildren(path);
        } catch (ZkNoNodeException e ){
            return null;
        } catch (Throwable e2 ){
            throw e2;
        }
        return null;
    }

    /**
     * Check if the given path exists
     */
    public Boolean pathExists(ZkClient client, String path) {
        return client.exists(path);
    }

    public Cluster getCluster(ZkClient zkClient) {
        val cluster = new Cluster;
        val nodes = getChildrenParentMayNotExist(zkClient, BrokerIdsPath);
        for (node< -nodes) {
            val brokerZKString = readData(zkClient, BrokerIdsPath + "/" + node)._1;
            cluster.add(Broker.createBroker(node.toInt, brokerZKString));
        }
        return cluster;
    }

    public void getPartitionLeaderAndIsrForTopics(ZkClient zkClient, Set topicAndPartitions<TopicAndPartition>);
  :mutable.Map<TopicAndPartition, LeaderIsrAndControllerEpoch> =

    {
        val ret = new mutable.HashMap<TopicAndPartition, LeaderIsrAndControllerEpoch>
        for (topicAndPartition< -topicAndPartitions) {
            ReplicationUtils.getLeaderIsrAndEpochForPartition(zkClient, topicAndPartition.topic, topicAndPartition.partition)
            match {
                case Some(leaderIsrAndControllerEpoch) =>ret.put(topicAndPartition, leaderIsrAndControllerEpoch);
                case None =>
            }
        }
        ret;
    }

    public void getReplicaAssignmentForTopics(ZkClient zkClient, Seq topics<String>):mutable.Map<TopicAndPartition, Seq<Integer>> =

    {
        val ret = new mutable.HashMap<TopicAndPartition, Seq<Integer>>
        topics.foreach {
        topic =>
        val jsonPartitionMapOpt = readDataMaybeNull(zkClient, getTopicPath(topic))._1;
        jsonPartitionMapOpt match {
            case Some(jsonPartitionMap) =>
                Json.parseFull(jsonPartitionMap) match {
                case Some(m) =>m.asInstanceOf<Map<String, Any>>.get ("partitions") match {
                    case Some(repl) =>
                        val replicaMap = repl.asInstanceOf < Map < String, Seq<Integer >>>
                        for ((partition, replicas) <-replicaMap){
                        ret.put(TopicAndPartition(topic, partition.toInt), replicas);
                        debug(String.format("Replicas assigned to topic <%s>, partition <%s> are <%s>", topic, partition, replicas))
                    }
                    case None =>
                }
                case None =>
            }
            case None =>
        }
    }
        ret;
    }

    public void getPartitionAssignmentForTopics(ZkClient zkClient, Seq topics<String>):
    mutable.Map<String, collection.Map<Integer,> Seq<Integer>>>=

    {
        val ret = new mutable.HashMap<String, Map<Integer,> Seq<Integer>>>();
        topics.foreach {
        topic =>
        val jsonPartitionMapOpt = readDataMaybeNull(zkClient, getTopicPath(topic))._1;
        val partitionMap = jsonPartitionMapOpt match {
            case Some(jsonPartitionMap) =>
                Json.parseFull(jsonPartitionMap) match {
                case Some(m) =>m.asInstanceOf<Map<String, Any>>.get ("partitions") match {
                    case Some(replicaMap) =>
                        val m1 = replicaMap.asInstanceOf < Map < String, Seq<Integer >>>
                            m1.map(p = > (p._1.toInt, p._2));
                    case None =>Map<Integer,> Seq<Integer >> ();
                }
                case None =>Map<Integer,> Seq<Integer >> ();
            }
            case None =>Map<Integer,> Seq<Integer >> ();
        }
        debug(String.format("Partition map for /brokers/topics/%s is %s", topic, partitionMap))
        ret += (topic -> partitionMap);
    }
        ret;
    }

    public void getPartitionsForTopics(ZkClient zkClient, Seq topics<String>):mutable.Map<String, Seq<Integer>> =

    {
        getPartitionAssignmentForTopics(zkClient, topics).map {
        topicAndPartitionMap =>
        val topic = topicAndPartitionMap._1;
        val partitionMap = topicAndPartitionMap._2;
        debug(String.format("partition assignment of /brokers/topics/%s is %s", topic, partitionMap))
        (topic -> partitionMap.keys.toSeq.sortWith((s, t) = > s < t));
    }
    }

    public void getPartitionsBeingReassigned(ZkClient zkClient):Map<TopicAndPartition, ReassignedPartitionsContext> =

    {
        // read the partitions and their new replica list;
        val jsonPartitionMapOpt = readDataMaybeNull(zkClient, ReassignPartitionsPath)._1;
        jsonPartitionMapOpt match {
        case Some(jsonPartitionMap) =>
            val reassignedPartitions = parsePartitionReassignmentData(jsonPartitionMap);
            reassignedPartitions.map(p = > (p._1 ->new ReassignedPartitionsContext(p._2)));
        case None =>Map.empty<TopicAndPartition, ReassignedPartitionsContext>
    }
    }

    // Parses without deduplicating keys so the the data can be checked before allowing reassignment to proceed;
    public void parsePartitionReassignmentDataWithoutDedup(String jsonData):Seq<(TopicAndPartition,Seq<Integer>)>=

    {
        Json.parseFull(jsonData) match {
        case Some(m) =>
            m.asInstanceOf<Map<String, Any>>.get ("partitions") match {
            case Some(partitionsSeq) =>
                partitionsSeq.asInstanceOf<Seq<Map<String, Any>>>.map (p = > {
                    val topic = p.get("topic").get.asInstanceOf < String >
                    val partition = p.get("partition").get.asInstanceOf < Integer >
                    val newReplicas = p.get("replicas").get.asInstanceOf < Seq < Integer >>
                    TopicAndPartition(topic, partition) ->newReplicas;
            });
            case None =>
                Seq.empty;
        }
        case None =>
            Seq.empty;
    }
    }

    public void parsePartitionReassignmentData(String jsonData):Map<TopicAndPartition, Seq<Integer>> =

    {
        parsePartitionReassignmentDataWithoutDedup(jsonData).toMap;
    }

    public void parseTopicsData(String jsonData):Seq<String> =

    {
        var topics = List.empty < String >
                Json.parseFull(jsonData) match {
        case Some(m) =>
            m.asInstanceOf<Map<String, Any>>.get ("topics") match {
            case Some(partitionsSeq) =>
                val mapPartitionSeq = partitionsSeq.asInstanceOf < Seq < Map < String, Any>>>
                mapPartitionSeq.foreach(p = > {
                        val topic = p.get("topic").get.asInstanceOf < String >
                        topics++ = List(topic);
                    });
            case None =>
        }
        case None =>
    }
        topics;
    }

    public String

    void getPartitionReassignmentZkData(Map partitionsToBeReassigned<TopicAndPartition, Seq<Integer>>) {
        Json.encode(Map("version" ->1, "partitions" ->partitionsToBeReassigned.map(e = > Map("topic" ->
        e._1.topic, "partition" ->e._1.partition,
                "replicas" ->e._2))));
    }

    public void updatePartitionReassignmentData(ZkClient zkClient, Map partitionsToBeReassigned<TopicAndPartition, Seq<Integer>>) {
        val zkPath = ZkUtils.ReassignPartitionsPath;
        partitionsToBeReassigned.size match {
            case 0 => // need to delete the /admin/reassign_partitions path;
                deletePath(zkClient, zkPath);
                info(String.format("No more partitions need to be reassigned. Deleting zk path %s", zkPath))
            case _ =>
                val jsonData = getPartitionReassignmentZkData(partitionsToBeReassigned);
                try {
                    updatePersistentPath(zkClient, zkPath, jsonData);
                    info(String.format("Updated partition reassignment path with %s", jsonData))
                } catch {
                case ZkNoNodeException nne =>
                    ZkUtils.createPersistentPath(zkClient, zkPath, jsonData);
                    debug(String.format("Created path %s with %s for partition reassignment", zkPath, jsonData))
                case Throwable e2 =>throw new AdminOperationException(e2.toString);
            }
        }
    }

    public void getPartitionsUndergoingPreferredReplicaElection(ZkClient zkClient):Set<TopicAndPartition> =

    {
        // read the partitions and their new replica list;
        val jsonPartitionListOpt = readDataMaybeNull(zkClient, PreferredReplicaLeaderElectionPath)._1;
        jsonPartitionListOpt match {
        case Some(jsonPartitionList) =>PreferredReplicaLeaderElectionCommand.parsePreferredReplicaElectionData(jsonPartitionList);
        case None =>Set.empty<TopicAndPartition>
    }
    }

    public void deletePartition(zkClient :ZkClient, Integer brokerId, String topic) {
        val brokerIdPath = BrokerIdsPath + "/" + brokerId;
        zkClient.delete(brokerIdPath);
        val brokerPartTopicPath = BrokerTopicsPath + "/" + topic + "/" + brokerId;
        zkClient.delete(brokerPartTopicPath);
    }

    public void getConsumersInGroup(ZkClient zkClient, String group):Seq<String> =

    {
        val dirs = new ZKGroupDirs(group);
        getChildren(zkClient, dirs.consumerRegistryDir);
    }

    public void getConsumersPerTopic(ZkClient zkClient, String group, Boolean excludeInternalTopics) :mutable.Map<String, List<ConsumerThreadId>> =

    {
        val dirs = new ZKGroupDirs(group);
        val consumers = getChildrenParentMayNotExist(zkClient, dirs.consumerRegistryDir);
        val consumersPerTopicMap = new mutable.HashMap<String, List<ConsumerThreadId>>
        for (consumer< -consumers) {
            val topicCount = TopicCount.constructTopicCount(group, consumer, zkClient, excludeInternalTopics);
            for ((topic, consumerThreadIdSet) <-topicCount.getConsumerThreadIdsPerTopic){
                for (consumerThreadId< -consumerThreadIdSet)
                    consumersPerTopicMap.get(topic) match {
                    case Some(curConsumers) =>consumersPerTopicMap.put(topic, consumerThreadId::curConsumers);
                    case _ =>consumersPerTopicMap.put(topic, List(consumerThreadId));
                }
            }
        }
        for ((topic, consumerList) <-consumersPerTopicMap )
        consumersPerTopicMap.put(topic, consumerList.sortWith((s, t) = > s < t));
        consumersPerTopicMap;
    }

    /**
     * This API takes in a broker id, queries zookeeper for the broker metadata and returns the metadata for that broker
     * or throws an exception if the broker dies before the query to zookeeper finishes
     *
     * @param brokerId The broker id
     * @param zkClient The zookeeper client connection
     * @return An optional Broker object encapsulating the broker metadata
     */
    public void getBrokerInfo(ZkClient zkClient, Integer brokerId):Option<Broker> =

    {
        ZkUtils.readDataMaybeNull(zkClient, ZkUtils.BrokerIdsPath + "/" + brokerId)._1 match {
        case Some(brokerInfo) =>Some(Broker.createBroker(brokerId, brokerInfo));
        case None =>None;
    }
    }

    public void getAllTopics(ZkClient zkClient):Seq<String> =

    {
        val topics = ZkUtils.getChildrenParentMayNotExist(zkClient, BrokerTopicsPath);
        if (topics == null)
            Seq.empty<String>
        else ;
        topics;
    }

    public void getAllPartitions(ZkClient zkClient):Set<TopicAndPartition> =

    {
        val topics = ZkUtils.getChildrenParentMayNotExist(zkClient, BrokerTopicsPath);
        if (topics == null) Set.empty<TopicAndPartition>
        else {
            topics.map {
                topic =>
                getChildren(zkClient, getTopicPartitionsPath(topic)).map(_.toInt).map(TopicAndPartition(topic, _));
            }.flatten.toSet;
        }
    }
}

    object ZKStringSerializer extends ZkSerializer{

        @throws(classOf<ZkMarshallingError>)
public void serialize(data:Object):Array<Byte> =data.asInstanceOf<String>.getBytes("UTF-8");

        @throws(classOf<ZkMarshallingError>)
public Object void deserialize(bytes:Array<Byte>){
        if(bytes==null)
        null;
        else;
        new String(bytes,"UTF-8");
        }
        }

class ZKGroupDirs(val String group){
public void consumerDir=ZkUtils.ConsumersPath;
public void consumerGroupDir=consumerDir+"/"+group;
public void consumerRegistryDir=consumerGroupDir+"/ids";
        }

class ZKGroupTopicDirs(String group,String topic)extends ZKGroupDirs(group){
public void consumerOffsetDir=consumerGroupDir+"/offsets/"+topic;
public void consumerOwnerDir=consumerGroupDir+"/owners/"+topic;
        }


class ZKConfig {
    public VerifiableProperties props;
    /**
     * ZK host string
     */
    public String zkConnect;

    /**
     * zookeeper session timeout
     */
    public Integer zkSessionTimeoutMs;

    /**
     * the max time that the client waits to establish a connection to zookeeper
     */
    public Integer zkConnectionTimeoutMs;

    /**
     * how far a ZK follower can be behind a ZK leader
     */
    public Integer zkSyncTimeMs;

    public ZKConfig(VerifiableProperties props) {
        this.props = props;
        zkConnect = props.getString("zookeeper.connect");
        zkSessionTimeoutMs = props.getInt("zookeeper.session.timeout.ms", 6000);
        zkConnectionTimeoutMs = props.getInt("zookeeper.connection.timeout.ms", zkSessionTimeoutMs);
        zkSyncTimeMs = props.getInt("zookeeper.sync.time.ms", 2000);
    }


}