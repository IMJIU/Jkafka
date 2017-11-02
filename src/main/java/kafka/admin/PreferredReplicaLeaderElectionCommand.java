package kafka.admin;

import kafka.log.TopicAndPartition;
import kafka.utils.Logging;

import java.util.Set;

/**
 * @author zhoulf
 * @create 2017-11-01 54 17
 **/

public class PreferredReplicaLeaderElectionCommand extends Logging {

       public void main(String[] args) {
           OptionParser parser = new OptionParser;
        val jsonFileOpt = parser.accepts("path-to-json-file", "The JSON file with the list of partitions " +
        "for which preferred replica leader election should be done, in the following format - \n" +
        "{\"partitions\":\n\t[{\"topic\": \"foo\", \"partition\": 1},\n\t {\"topic\": \"foobar\", \"partition\": 2}]\n}\n" +
        "Defaults to all existing partitions");
        .withRequiredArg;
        .describedAs("list of partitions for which preferred replica leader election needs to be triggered")
        .ofType(classOf<String>);
        val zkConnectOpt = parser.accepts("zookeeper", "The REQUIRED connection string for the zookeeper connection in the " +
        "form port host. Multiple URLS can be given to allow fail-over.")
        .withRequiredArg;
        .describedAs("urls");
        .ofType(classOf<String>);

        if(args.length == 0)
        CommandLineUtils.printUsageAndDie(parser, "This tool causes leadership for each partition to be transferred back to the 'preferred replica'," +
        " it can be used to balance leadership among the servers.");

        val options = parser.parse(args : _*);

        CommandLineUtils.checkRequiredArgs(parser, options, zkConnectOpt);

        val zkConnect = options.valueOf(zkConnectOpt);
        var ZkClient zkClient = null;

        try {
        zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer);
        val partitionsForPreferredReplicaElection =
        if (!options.has(jsonFileOpt))
        ZkUtils.getAllPartitions(zkClient);
        else;
        parsePreferredReplicaElectionData(Utils.readFileAsString(options.valueOf(jsonFileOpt)));
        val preferredReplicaElectionCommand = new PreferredReplicaLeaderElectionCommand(zkClient, partitionsForPreferredReplicaElection);

        preferredReplicaElectionCommand.moveLeaderToPreferredReplica();
        println(String.format("Successfully started preferred replica election for partitions %s",partitionsForPreferredReplicaElection))
        } catch {
        case Throwable e =>
        println("Failed to start preferred replica election");
        println(Utils.stackTrace(e));
        } finally {
        if (zkClient != null)
        zkClient.close();
        }
        }

       public static Set<TopicAndPartition> parsePreferredReplicaElectionData(String jsonString) {
        Json.parseFull(jsonString) match {
        case Some(m) =>
        m.asInstanceOf<Map<String, Any>>.get("partitions") match {
        case Some(partitionsList) =>
        val partitionsRaw = partitionsList.asInstanceOf<List<Map[String, Any]>>
        val partitions = partitionsRaw.map { p =>
        val topic = p.get("topic").get.asInstanceOf<String>
        val partition = p.get("partition").get.asInstanceOf<Integer>
        TopicAndPartition(topic, partition);
        }
        val duplicatePartitions = Utils.duplicates(partitions);
        val partitionsSet = partitions.toSet;
        if (duplicatePartitions.nonEmpty)
        throw new AdminOperationException(String.format("Preferred replica election data contains duplicate partitions: %s",duplicatePartitions.mkString(",")))
        partitionsSet;
        case None => throw new AdminOperationException("Preferred replica election data is empty");
        }
        case None => throw new AdminOperationException("Preferred replica election data is empty");
        }
        }

       public void writePreferredReplicaElectionData(ZkClient zkClient,
        scala partitionsUndergoingPreferredReplicaElection.collection.Set<TopicAndPartition>) {
        val zkPath = ZkUtils.PreferredReplicaLeaderElectionPath;
        val partitionsList = partitionsUndergoingPreferredReplicaElection.map(e => Map("topic" -> e.topic, "partition" -> e.partition));
        val jsonData = Json.encode(Map("version" -> 1, "partitions" -> partitionsList));
        try {
        ZkUtils.createPersistentPath(zkClient, zkPath, jsonData);
        info(String.format("Created preferred replica election path with %s",jsonData))
        } catch {
        case ZkNodeExistsException nee =>
        val partitionsUndergoingPreferredReplicaElection =
        PreferredReplicaLeaderElectionCommand.parsePreferredReplicaElectionData(ZkUtils.readData(zkClient, zkPath)._1);
        throw new AdminOperationException("Preferred replica leader election currently in progress for " +
        String.format("%s. Aborting operation",partitionsUndergoingPreferredReplicaElection))
        case Throwable e2 => throw new AdminOperationException(e2.toString);
        }
        }
        }

class PreferredReplicaLeaderElectionCommand(ZkClient zkClient, scala partitions.collection.Set<TopicAndPartition>);
        extends Logging {
       public void moveLeaderToPreferredReplica() = {
        try {
        val validPartitions = partitions.filter(p => validatePartition(zkClient, p.topic, p.partition));
        PreferredReplicaLeaderElectionCommand.writePreferredReplicaElectionData(zkClient, validPartitions);
        } catch {
        case Throwable e => throw new AdminCommandFailedException("Admin command failed", e);
        }
        }

       public Boolean  void validatePartition(ZkClient zkClient, String topic, Int partition) {
        // check if partition exists;
        val partitionsOpt = ZkUtils.getPartitionsForTopics(zkClient, List(topic)).get(topic);
        partitionsOpt match {
        case Some(partitions) =>
        if(partitions.contains(partition)) {
        true;
        } else {
        error(String.format("Skipping preferred replica leader election for partition <%s,%d> ",topic, partition) +
        "since it doesn't exist");
        false;
        }
        case None => error("Skipping preferred replica leader election for partition " +
        String.format("<%s,%d> since topic %s doesn't exist",topic, partition, topic))
        false;
        }
        }
        }
