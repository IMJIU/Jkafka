package kafka.server;

import com.google.common.collect.ImmutableMap;
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

public class SimpleFetchTest {

    List<KafkaConfig> configs;
    String topic = "foo";
    Integer partitionId = 0;

    @Before
    public void setup() {
        configs = Sc.map(TestUtils.createBrokerConfigs(2, null), p -> {
            KafkaConfig config = new KafkaConfig(p);
            config.replicaLagTimeMaxMs = 100L;
            config.replicaFetchWaitMaxMs = 100;
            config.replicaLagMaxMessages = 10L;
            return config;
        });
    }

    /**
     * The scenario for this test is that there is one topic, "test-topic", one broker "0" that has
     * one  partition with one follower replica on broker "1".  The leader replica on "0"
     * has HW of "5" and LEO of "20".  The follower on broker "1" has a local replica
     * with a HW matching the leader's ("5") and LEO of "15", meaning it's not in-sync
     * but is still in ISR (hasn't yet expired from ISR).
     * <p>
     * When a normal consumer fetches data, it should only see data up to the HW of the leader,
     * in this case up an offset of "5".
     */
    @Test
    public void testNonReplicaSeesHwWhenFetching() {
    /* setup */
        MockTime time = new MockTime();
        Long leo = 20L;
        Long hw = 5L;
        Integer fetchSize = 100;
        Message messages = new Message("test-message".getBytes());

        // create nice mock since we don't particularly care about zkclient calls;
        ZkClient zkClient = EasyMock.createNiceMock(ZkClient.class);
        EasyMock.expect(zkClient.exists(ZkUtils.ControllerEpochPath)).andReturn(false);
        EasyMock.replay(zkClient);

        Log log = EasyMock.createMock(Log.class);
        EasyMock.expect(log.logEndOffset()).andReturn(leo).anyTimes();
        EasyMock.expect(log);
        EasyMock.expect(log.read(0L, fetchSize, Optional.of(hw))).andReturn(
                new FetchDataInfo(
                        new LogOffsetMetadata(0L, 0L, leo.intValue()),
                        new ByteBufferMessageSet(messages)
                )).anyTimes();
        EasyMock.replay(log);

        LogManager logManager = EasyMock.createMock(LogManager.class);
        EasyMock.expect(logManager.getLog(new TopicAndPartition(topic, partitionId))).andReturn(Optional.of(log)).anyTimes();
        EasyMock.replay(logManager);

        ReplicaManager replicaManager = EasyMock.createMock(kafka.server.ReplicaManager.class);
        replicaManager.config = Sc.head(configs);
        replicaManager.logManager = logManager;
        replicaManager.replicaFetcherManager = EasyMock.createMock(ReplicaFetcherManager.class);
        replicaManager.zkClient = zkClient;
//        EasyMock.expect(replicaManager.config).andReturn(Sc.head(configs));
//        EasyMock.expect(replicaManager.logManager).andReturn(logManager);
//        EasyMock.expect(replicaManager.replicaFetcherManager).andReturn(EasyMock.createMock(ReplicaFetcherManager.class));
//        EasyMock.expect(replicaManager.zkClient).andReturn(zkClient);
        FetchDataInfo fetchInfo = log.read(0L, fetchSize, Optional.of(hw));
        FetchResponsePartitionData partitionData = new FetchResponsePartitionData(ErrorMapping.NoError, hw, fetchInfo.messageSet);
        EasyMock.expect(replicaManager.readMessageSets(EasyMock.anyObject())).andReturn(
                Sc.toMap(new TopicAndPartition(topic, partitionId), new PartitionDataAndOffset(partitionData, fetchInfo.fetchOffset))
        ).anyTimes();
        EasyMock.replay(replicaManager);

        Partition partition = getPartitionWithAllReplicasInISR(topic, partitionId, time, Sc.head(configs).brokerId, log, hw, replicaManager);
        partition.getReplica(configs.get(1).brokerId).get().logEndOffset_(new LogOffsetMetadata(leo - 5L, 0L, leo.intValue() - 5));

        EasyMock.reset(replicaManager);
//        EasyMock.expect(replicaManager.config).andReturn(Sc.head(configs)).anyTimes();
        EasyMock.expect(replicaManager.getLeaderReplicaIfLocal(topic, partitionId)).andReturn(partition.leaderReplicaIfLocal().get()).anyTimes();
        replicaManager.initWithRequestPurgatory(EasyMock.anyObject(), EasyMock.anyObject());
        fetchInfo = log.read(0L, fetchSize, Optional.of(hw));
        partitionData = new FetchResponsePartitionData(ErrorMapping.NoError, hw, fetchInfo.messageSet);
        EasyMock.expect(replicaManager.readMessageSets(EasyMock.anyObject())).andReturn(
                Sc.toMap(new TopicAndPartition(topic, partitionId), new PartitionDataAndOffset(partitionData, fetchInfo.fetchOffset))
        ).anyTimes();

        EasyMock.replay(replicaManager);

        OffsetManager offsetManager = EasyMock.createMock(kafka.server.OffsetManager.class);

        KafkaController controller = EasyMock.createMock(kafka.controller.KafkaController.class);

        // start a request channel with 2 processors and a queue size of 5 (this is more or less arbitrary);
        // don't provide replica or leader callbacks since they will not be tested here;
        RequestChannel requestChannel = new RequestChannel(2, 5);
        KafkaApis apis = new KafkaApis(requestChannel, replicaManager, offsetManager, zkClient, Sc.head(configs).brokerId, Sc.head(configs), controller);

        PartitionStateInfo partitionStateInfo = EasyMock.createNiceMock(PartitionStateInfo.class);
        apis.metadataCache.addOrUpdatePartitionInfo(topic, partitionId, partitionStateInfo);
        EasyMock.replay(partitionStateInfo);
        // This request (from a follower) wants to read up to 2*HW but should only get back up to HW bytes into the log;
        FetchRequest goodFetch = new FetchRequestBuilder()
                .replicaId(Request.OrdinaryConsumerId)
                .addFetch(topic, partitionId, 0L, fetchSize)
                .build();
        ByteBuffer goodFetchBB = TestUtils.createRequestByteBuffer(goodFetch);

        // send the request;
        apis.handleFetchRequest(new RequestChannel.Request(1, 5, goodFetchBB, 1L));

        // make sure the log only reads bytes between 0->HW (5);
        EasyMock.verify(log);
    }

    /**
     * The scenario for this test is that there is one topic, "test-topic", on broker "0" that has
     * one  partition with one follower replica on broker "1".  The leader replica on "0"
     * has HW of "5" and LEO of "20".  The follower on broker "1" has a local replica
     * with a HW matching the leader's ("5") and LEO of "15", meaning it's not in-sync
     * but is still in ISR (hasn't yet expired from ISR).
     * <p>
     * When the follower from broker "1" fetches data, it should see data upto the log end offset ("20")
     */
    @Test
    public void testReplicaSeesLeoWhenFetching() {
    /* setup */
        MockTime time = new MockTime();
        long leo = 20L;
        long hw = 5L;

        Message messages = new Message("test-message".getBytes());

        Integer followerReplicaId = configs.get(1).brokerId;
        Long followerLEO = 15L;

        ZkClient zkClient = EasyMock.createNiceMock(ZkClient.class);
        EasyMock.expect(zkClient.exists(ZkUtils.ControllerEpochPath)).andReturn(false);
        EasyMock.replay(zkClient);

        Log log = EasyMock.createMock(Log.class);
        EasyMock.expect(log.logEndOffset()).andReturn(leo).anyTimes();
        EasyMock.expect(log.read(followerLEO, Integer.MAX_VALUE, Optional.empty())).andReturn(
                new FetchDataInfo(new LogOffsetMetadata(followerLEO, 0L, followerLEO.intValue()),new ByteBufferMessageSet(messages)
                )).anyTimes();
        EasyMock.replay(log);

        LogManager logManager = EasyMock.createMock(LogManager.class);
        EasyMock.expect(logManager.getLog(new TopicAndPartition(topic, 0))).andReturn(Optional.of(log)).anyTimes();
        EasyMock.replay(logManager);

        ReplicaManager replicaManager = EasyMock.createMock(ReplicaManager.class);
        replicaManager.config = Sc.head(configs);
        replicaManager.logManager = logManager;
        replicaManager.replicaFetcherManager = EasyMock.createMock(ReplicaFetcherManager.class);
        replicaManager.zkClient = zkClient;
//        EasyMock.expect(replicaManager.config).andReturn(configs.get(0));
//        EasyMock.expect(replicaManager.logManager).andReturn(logManager);
//        EasyMock.expect(replicaManager.replicaFetcherManager).andReturn(EasyMock.createMock(ReplicaFetcherManager.class));
//        EasyMock.expect(replicaManager.zkClient).andReturn(zkClient);

        FetchDataInfo fetchInfo = log.read(followerLEO, Integer.MAX_VALUE, Optional.empty());
        FetchResponsePartitionData partitionData = new FetchResponsePartitionData(ErrorMapping.NoError, hw, fetchInfo.messageSet);
        EasyMock.expect(replicaManager.readMessageSets(EasyMock.anyObject())).andReturn(
                ImmutableMap.of(new TopicAndPartition(topic, partitionId),
                        new PartitionDataAndOffset(partitionData, fetchInfo.fetchOffset))
        ).anyTimes();
        EasyMock.replay(replicaManager);

        Partition partition = getPartitionWithAllReplicasInISR(topic, partitionId, time, configs.get(0).brokerId, log, hw, replicaManager);
        partition.getReplica(followerReplicaId).get().logEndOffset_(new LogOffsetMetadata(followerLEO, 0L, followerLEO.intValue()));

        EasyMock.reset(replicaManager);
//        EasyMock.expect(replicaManager.config).andReturn(configs.get(0)).anyTimes();
        replicaManager.updateReplicaLEOAndPartitionHW(topic, partitionId, followerReplicaId, new LogOffsetMetadata(followerLEO, 0L, followerLEO.intValue()));
//        EasyMock.expect(replicaManager.updateReplicaLEOAndPartitionHW(topic, partitionId, followerReplicaId, new LogOffsetMetadata(followerLEO, 0L, followerLEO.intValue())));
        Replica replica = Sc.find(partition.inSyncReplicas, r -> r.brokerId == configs.get(1).brokerId);
        EasyMock.expect(replicaManager.getReplica(topic, partitionId, followerReplicaId)).andReturn(Optional.of(replica));
        EasyMock.expect(replicaManager.getLeaderReplicaIfLocal(topic, partitionId)).andReturn(partition.leaderReplicaIfLocal().get()).anyTimes();
        replicaManager.initWithRequestPurgatory(EasyMock.anyObject(), EasyMock.anyObject());
//        EasyMock.expect(replicaManager.initWithRequestPurgatory(EasyMock.anyObject(), EasyMock.anyObject()));
        fetchInfo = log.read(followerLEO, Integer.MAX_VALUE, Optional.empty());
        partitionData = new FetchResponsePartitionData(ErrorMapping.NoError, hw, fetchInfo.messageSet);
        EasyMock.expect(replicaManager.readMessageSets(EasyMock.anyObject())).andReturn(
                ImmutableMap.of(new TopicAndPartition(topic, partitionId),
                        new PartitionDataAndOffset(partitionData, fetchInfo.fetchOffset))
        ).anyTimes();
//        EasyMock.expect(replicaManager.unblockDelayedProduceRequests(EasyMock.anyObject())).anyTimes();
        replicaManager.unblockDelayedProduceRequests(EasyMock.anyObject());
        EasyMock.replay(replicaManager);

        OffsetManager offsetManager = EasyMock.createMock(OffsetManager.class);

        KafkaController controller = EasyMock.createMock(KafkaController.class);

        RequestChannel requestChannel = new RequestChannel(2, 5);
        KafkaApis apis = new KafkaApis(requestChannel, replicaManager, offsetManager, zkClient, configs.get(0).brokerId, configs.get(0), controller);
        PartitionStateInfo partitionStateInfo = EasyMock.createNiceMock(PartitionStateInfo.class);
        apis.metadataCache.addOrUpdatePartitionInfo(topic, partitionId, partitionStateInfo);
        EasyMock.replay(partitionStateInfo);

        /**
         * This fetch, coming from a replica, requests all data at offset "15".  Because the request is coming
         * from a follower, the leader should oblige and read beyond the HW.
         */
        FetchRequest bigFetch = new FetchRequestBuilder()
                .replicaId(followerReplicaId)
                .addFetch(topic, partitionId, followerLEO, Integer.MAX_VALUE)
                .build();

        ByteBuffer fetchRequestBB = TestUtils.createRequestByteBuffer(bigFetch);
        // send the request;
        apis.handleFetchRequest(new RequestChannel.Request(0, 5, fetchRequestBB, 1L));
        /**
         * Make sure the log satisfies the fetch from a follower by reading data beyond the HW, mainly all bytes after
         * an offset of 15
         */
        EasyMock.verify(log);
    }

    private Partition getPartitionWithAllReplicasInISR(String topic, Integer partitionId, Time time, Integer leaderId,
                                                       Log localLog, Long leaderHW, ReplicaManager replicaManager) {
        Partition partition = new Partition(topic, partitionId, time, replicaManager);
        Replica leaderReplica = new Replica(leaderId, partition, time, 0L, Optional.of(localLog));
        List<Replica> allReplicas = getFollowerReplicas(partition, leaderId, time);
        allReplicas.add(leaderReplica);
        allReplicas.forEach(r -> partition.addReplicaIfNotExists(r));
        // set in sync replicas for this partition to all the assigned replicas;
        partition.inSyncReplicas = Sc.toSet(allReplicas);
        // set the leader and its hw and the hw update time;
        partition.leaderReplicaIdOpt = Optional.of(leaderId);
        leaderReplica.highWatermark_(new LogOffsetMetadata(leaderHW));
        return partition;
    }

    private List<Replica> getFollowerReplicas(Partition partition, Integer leaderId, Time time) {
        return Sc.map(
                Sc.filter(configs, c -> c.brokerId != leaderId),
                config -> new Replica(config.brokerId, partition, time));
    }

    @Test
    public void t_mock() {
        ReplicaManager replicaManager = EasyMock.createMock(ReplicaManager.class);
        replicaManager.checkpointHighWatermarks();
    }
}
