package kafka.server;


import kafka.api.*;
import kafka.cluster.Partition;
import kafka.cluster.Replica;
import kafka.common.*;
import kafka.controller.KafkaController;
import kafka.log.Log;
import kafka.log.LogManager;
import kafka.log.TopicAndPartition;
import kafka.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.network.RequestChannel;
import kafka.utils.*;
import org.I0Itec.zkclient.ZkClient;
import org.easymock.EasyMock;
import org.junit.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

import static kafka.utils.TestUtils.*;

public class IsrExpirationTest {

   Map<<String, Integer>, List<Integer>> topicPartitionIsr = new HashMap<<String, Integer>, Seq<Integer>>();
  val configs = TestUtils.createBrokerConfigs(2).map(new KafkaConfig(_) {
     @Override val replicaLagTimeMaxMs = 100L
     @Override val replicaFetchWaitMaxMs = 100
     @Override val replicaLagMaxMessages = 10L
  });
  val topic = "foo";

 public void testIsrExpirationForStuckFollowers() {
    val time = new MockTime;
    val log = getLogWithLogEndOffset(15L, 2) // set logEndOffset for leader to 15L;

    // create one partition and all replicas;
    val partition0 = getPartitionWithAllReplicasInIsr(topic, 0, time, configs.head, log);
   Assert.assertEquals("All replicas should be in ISR", configs.map(_.brokerId).toSet, partition0.inSyncReplicas.map(_.brokerId));
    val leaderReplica = partition0.getReplica(configs.head.brokerId).get;

    // let the follower catch up to 10;
    (partition0.assignedReplicas() - leaderReplica).foreach(r -> r.logEndOffset = new LogOffsetMetadata(10L))
    var partition0OSR = partition0.getOutOfSyncReplicas(leaderReplica, configs.head.replicaLagTimeMaxMs, configs.head.replicaLagMaxMessages);
   Assert.assertEquals("No replica should be out of sync", Set.empty<Integer>, partition0OSR.map(_.brokerId));

    // let some time pass;
    time.sleep(150);

    // now follower (broker id 1) has caught up to only 10, while the leader is at 15 AND the follower hasn't;
    // pulled any data for > replicaMaxLagTimeMs ms. So it is stuck;
    partition0OSR = partition0.getOutOfSyncReplicas(leaderReplica, configs.head.replicaLagTimeMaxMs, configs.head.replicaLagMaxMessages);
   Assert.assertEquals("Replica 1 should be out of sync", Set(configs.last.brokerId), partition0OSR.map(_.brokerId));
    EasyMock.verify(log)
  }

 public void testIsrExpirationForSlowFollowers() {
    val time = new MockTime;
    // create leader replica;
    val log = getLogWithLogEndOffset(15L, 1);
    // add one partition;
    val partition0 = getPartitionWithAllReplicasInIsr(topic, 0, time, configs.head, log);
   Assert.assertEquals("All replicas should be in ISR", configs.map(_.brokerId).toSet, partition0.inSyncReplicas.map(_.brokerId));
    val leaderReplica = partition0.getReplica(configs.head.brokerId).get;
    // set remote replicas leo to something low, like 4;
    (partition0.assignedReplicas() - leaderReplica).foreach(r -> r.logEndOffset = new LogOffsetMetadata(4L))

    // now follower (broker id 1) has caught up to only 4, while the leader is at 15. Since the gap it larger than;
    // replicaMaxLagBytes, the follower is out of sync.;
    val partition0OSR = partition0.getOutOfSyncReplicas(leaderReplica, configs.head.replicaLagTimeMaxMs, configs.head.replicaLagMaxMessages);
   Assert.assertEquals("Replica 1 should be out of sync", Set(configs.last.brokerId), partition0OSR.map(_.brokerId));

    EasyMock.verify(log)
  }

  privatepublic void getPartitionWithAllReplicasInIsr(String topic, Int partitionId, Time time, KafkaConfig config,
                                               Log localLog): Partition = {
    val leaderId=config.brokerId;
    val replicaManager = new ReplicaManager(config, time, null, null, null, new AtomicBoolean(false));
    val partition = replicaManager.getOrCreatePartition(topic, partitionId);
    val leaderReplica = new Replica(leaderId, partition, time, 0, Optional.of(localLog));

    val allReplicas = getFollowerReplicas(partition, leaderId, time) :+ leaderReplica;
    allReplicas.foreach(r -> partition.addReplicaIfNotExists(r))
    // set in sync replicas for this partition to all the assigned replicas;
    partition.inSyncReplicas = allReplicas.toSet;
    // set the leader and its hw and the hw update time;
    partition.leaderReplicaIdOpt = Optional.of(leaderId);
    partition;
  }

  privatepublic Log  void getLogWithLogEndOffset(Long logEndOffset, Int expectedCalls) {
    val log1 = EasyMock.createMock(classOf<kafka.log.Log>);
    EasyMock.expect(log1.logEndOffsetMetadata).andReturn(new LogOffsetMetadata(logEndOffset)).times(expectedCalls);
    EasyMock.replay(log1);

    log1;
  }

  privatepublic void getFollowerReplicas(Partition partition, Int leaderId, Time time): Seq<Replica> = {
    configs.filter(_.brokerId != leaderId).map { config ->
      new Replica(config.brokerId, partition, time);
    }
  }
}
