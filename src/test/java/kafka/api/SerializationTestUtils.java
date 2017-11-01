package kafka.api;

import com.google.common.collect.Lists;
import kafka.func.Tuple;
import kafka.log.TopicAndPartition;
import kafka.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.utils.Utils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by Administrator on 2017/10/11.
 */


public class SerializationTestUtils {
    private String topic1 = "test1";
    private String topic2 = "test2";
    private int leader1 = 0;
    private List<Integer> isr1 = Lists.newArrayList(0, 1, 2);
    private int leader2 = 0;
    private List<Integer> isr2 = Lists.newArrayList(0, 2, 3);
    private FetchResponsePartitionData partitionDataFetchResponse0 = new FetchResponsePartitionData(new ByteBufferMessageSet(new Message("first message".getBytes())));
    private FetchResponsePartitionData partitionDataFetchResponse1 = new FetchResponsePartitionData(new ByteBufferMessageSet(new Message("second message".getBytes())));
    private FetchResponsePartitionData partitionDataFetchResponse2 = new FetchResponsePartitionData(new ByteBufferMessageSet(new Message("third message".getBytes())));
    private FetchResponsePartitionData partitionDataFetchResponse3 = new FetchResponsePartitionData(new ByteBufferMessageSet(new Message("fourth message".getBytes())));
    private Map<Integer, FetchResponsePartitionData> partitionDataFetchResponseMap = new HashMap<Integer, FetchResponsePartitionData>() {{
        put(0, partitionDataFetchResponse0);
        put(1, partitionDataFetchResponse1);
        put(2, partitionDataFetchResponse2);
        put(3, partitionDataFetchResponse3);
    }};

    private Map<TopicAndPartition, FetchResponsePartitionData> topicDataFetchResponse;


    public void init() {
        List<Tuple<TopicAndPartition, FetchResponsePartitionData>> groupedData = Lists.newArrayList(topic1, topic2).stream().flatMap(topic ->
                partitionDataFetchResponseMap.entrySet().stream().map(e -> Tuple.of(new TopicAndPartition(topic, e.getKey()), e.getValue()))
        ).collect(Collectors.toList());
        topicDataFetchResponse = Utils.toMap(groupedData);
    }

    private ByteBufferMessageSet partitionDataMessage0 = new ByteBufferMessageSet(new Message("first message".getBytes()));
    private ByteBufferMessageSet partitionDataMessage1 = new ByteBufferMessageSet(new Message("second message".getBytes()));
    private ByteBufferMessageSet partitionDataMessage2 = new ByteBufferMessageSet(new Message("third message".getBytes()));
    private ByteBufferMessageSet partitionDataMessage3 = new ByteBufferMessageSet(new Message("fourth message".getBytes()));
    private List<ByteBufferMessageSet> partitionDataProducerRequestArray = Lists.newArrayList(partitionDataMessage0, partitionDataMessage1, partitionDataMessage2, partitionDataMessage3);
//
//private val topicDataProducerRequest = {
//        val groupedData = Array(topic1, topic2).flatMap(topic =>
//        partitionDataProducerRequestArray.zipWithIndex.map;
//        {
//        case(partitionDataMessage, partition) =>
//        (TopicAndPartition(topic, partition), partitionDataMessage);
//        });
//        collection.mutable.Map(_ groupedData*);
//        }
//
//private val requestInfos = collection.immutable.Map(
//        TopicAndPartition(topic1, 0) -> PartitionFetchInfo(1000, 100),
//        TopicAndPartition(topic1, 1) -> PartitionFetchInfo(2000, 100),
//        TopicAndPartition(topic1, 2) -> PartitionFetchInfo(3000, 100),
//        TopicAndPartition(topic1, 3) -> PartitionFetchInfo(4000, 100),
//        TopicAndPartition(topic2, 0) -> PartitionFetchInfo(1000, 100),
//        TopicAndPartition(topic2, 1) -> PartitionFetchInfo(2000, 100),
//        TopicAndPartition(topic2, 2) -> PartitionFetchInfo(3000, 100),
//        TopicAndPartition(topic2, 3) -> PartitionFetchInfo(4000, 100);
//        );
//
//private val brokers = List(new Broker(0, "localhost", 1011), new Broker(1, "localhost", 1012), new Broker(2, "localhost", 1013));
//private val partitionMetaData0 = new PartitionMetadata(0, Some(brokers.head), replicas = brokers, isr = brokers, errorCode = 0);
//private val partitionMetaData1 = new PartitionMetadata(1, Some(brokers.head), replicas = brokers, isr = brokers.tail, errorCode = 1);
//private val partitionMetaData2 = new PartitionMetadata(2, Some(brokers.head), replicas = brokers, isr = brokers, errorCode = 2);
//private val partitionMetaData3 = new PartitionMetadata(3, Some(brokers.head), replicas = brokers, isr = brokers.tail.tail, errorCode = 3);
//private val partitionMetaDataSeq = Seq(partitionMetaData0, partitionMetaData1, partitionMetaData2, partitionMetaData3);
//private val topicmetaData1 = new TopicMetadata(topic1, partitionMetaDataSeq);
//private val topicmetaData2 = new TopicMetadata(topic2, partitionMetaDataSeq);
//
//       public LeaderAndIsrRequest  void createTestLeaderAndIsrRequest()  {
//        val leaderAndIsr1 = new LeaderIsrAndControllerEpoch(new LeaderAndIsr(leader1, 1, isr1, 1), 1);
//        val leaderAndIsr2 = new LeaderIsrAndControllerEpoch(new LeaderAndIsr(leader2, 1, isr2, 2), 1);
//        val map = Map(((topic1, 0), PartitionStateInfo(leaderAndIsr1, isr1.toSet)),
//        ((topic2, 0), PartitionStateInfo(leaderAndIsr2, isr2.toSet)));
//        new LeaderAndIsrRequest(map.toMap, collection.immutable.Set<Broker>(), 0, 1, 0, "");
//        }
//
//       public LeaderAndIsrResponse  void createTestLeaderAndIsrResponse()  {
//        val responseMap = Map(((topic1, 0), ErrorMapping.NoError),
//        ((topic2, 0), ErrorMapping.NoError));
//        new LeaderAndIsrResponse(1, responseMap);
//        }
//
//       public StopReplicaRequest  void createTestStopReplicaRequest()  {
//        new StopReplicaRequest(controllerId = 0, controllerEpoch = 1, correlationId = 0, deletePartitions = true,
//        partitions = collection.immutable.Set(TopicAndPartition(topic1, 0),TopicAndPartition(topic2, 0)));
//        }
//
//       public StopReplicaResponse  void createTestStopReplicaResponse()  {
//        val responseMap = Map((TopicAndPartition(topic1, 0), ErrorMapping.NoError),
//        (TopicAndPartition(topic2, 0), ErrorMapping.NoError));
//        new StopReplicaResponse(0, responseMap.toMap);
//        }
//
//       public void ProducerRequest createTestProducerRequest = {
//        new ProducerRequest(1, "client 1", 0, 1000, topicDataProducerRequest);
//        }
//
//       public void ProducerResponse createTestProducerResponse =
//        ProducerResponse(1, Map(
//        TopicAndPartition(topic1, 0) -> ProducerResponseStatus(0.toShort, 10001),
//        TopicAndPartition(topic2, 0) -> ProducerResponseStatus(0.toShort, 20001);
//        ));
//
//       public void FetchRequest createTestFetchRequest = {
//        new FetchRequest(requestInfo = requestInfos);
//        }
//
//       public void FetchResponse createTestFetchResponse = {
//        FetchResponse(1, topicDataFetchResponse);
//        }
//
//       public void createTestOffsetRequest = new OffsetRequest(
//        collection.immutable.Map(TopicAndPartition(topic1, 1) -> PartitionOffsetRequestInfo(1000, 200)),
//        replicaId = 0;
//        );
//
//       public void OffsetResponse createTestOffsetResponse = {
//        new OffsetResponse(0, collection.immutable.Map(
//        TopicAndPartition(topic1, 1) -> PartitionOffsetsResponse(ErrorMapping.NoError, Seq(1000l, 2000l, 3000l, 4000l)));
//        );
//        }
//
//       public void TopicMetadataRequest createTestTopicMetadataRequest = {
//        new TopicMetadataRequest(1, 1, "client 1", Seq(topic1, topic2));
//        }
//
//       public void TopicMetadataResponse createTestTopicMetadataResponse = {
//        new TopicMetadataResponse(brokers, Seq(topicmetaData1, topicmetaData2), 1);
//        }
//
//       public void OffsetCommitRequest createTestOffsetCommitRequestV1 = {
//        new OffsetCommitRequest("group 1", collection.immutable.Map(
//        TopicAndPartition(topic1, 0) -> OffsetAndMetadata(offset=42L, metadata="some metadata", timestamp=SystemTime.milliseconds),
//        TopicAndPartition(topic1, 1) -> OffsetAndMetadata(offset=100L, metadata=OffsetAndMetadata.NoMetadata, timestamp=SystemTime.milliseconds);
//        ));
//        }
//
//       public void OffsetCommitRequest createTestOffsetCommitRequestV0 = {
//        new OffsetCommitRequest(
//        versionId = 0,
//        groupId = "group 1",
//        requestInfo = collection.immutable.Map(
//        TopicAndPartition(topic1, 0) -> OffsetAndMetadata(offset=42L, metadata="some metadata"),
//        TopicAndPartition(topic1, 1) -> OffsetAndMetadata(offset=100L, metadata=OffsetAndMetadata.NoMetadata);
//        ));
//        }
//
//       public void OffsetCommitResponse createTestOffsetCommitResponse = {
//        new OffsetCommitResponse(collection.immutable.Map(TopicAndPartition(topic1, 0) -> ErrorMapping.NoError,
//        TopicAndPartition(topic1, 1) -> ErrorMapping.NoError));
//        }
//
//       public void OffsetFetchRequest createTestOffsetFetchRequest = {
//        new OffsetFetchRequest("group 1", Seq(
//        TopicAndPartition(topic1, 0),
//        TopicAndPartition(topic1, 1);
//        ));
//        }
//
//       public void OffsetFetchResponse createTestOffsetFetchResponse = {
//        new OffsetFetchResponse(collection.immutable.Map(
//        TopicAndPartition(topic1, 0) -> OffsetMetadataAndError(42L, "some metadata", ErrorMapping.NoError),
//        TopicAndPartition(topic1, 1) -> OffsetMetadataAndError(100L, OffsetAndMetadata.NoMetadata, ErrorMapping.UnknownTopicOrPartitionCode);
//        ));
//        }
//
//       public void ConsumerMetadataRequest createConsumerMetadataRequest = {
//        ConsumerMetadataRequest("group 1", clientId = "client 1");
//        }
//
//       public void ConsumerMetadataResponse createConsumerMetadataResponse = {
//        ConsumerMetadataResponse(Some(brokers.head), ErrorMapping.NoError);
//        }
//
//       public void HeartbeatRequestAndHeader createHeartbeatRequestAndHeader = {
//        val body = new HeartbeatRequest("group1", 1, "consumer1");
//        HeartbeatRequestAndHeader(0.asInstanceOf<Short>, 1, "", body);
//        }
//
//       public void HeartbeatResponseAndHeader createHeartbeatResponseAndHeader = {
//        val body = new HeartbeatResponse(0.asInstanceOf<Short>);
//        HeartbeatResponseAndHeader(1, body);
//        }
//
//       public void JoinGroupRequestAndHeader createJoinGroupRequestAndHeader = {
//        import scala.collection.JavaConversions._;
//        val body = new JoinGroupRequest("group1", 30000, List("topic1"), "consumer1", "strategy1");
//        JoinGroupRequestAndHeader(0.asInstanceOf<Short>, 1, "", body);
//        }
//
//       public void JoinGroupResponseAndHeader createJoinGroupResponseAndHeader = {
//        import scala.collection.JavaConversions._;
//        val body = new JoinGroupResponse(0.asInstanceOf<Short>, 1, "consumer1", List(new TopicPartition("test11", 1)));
//        JoinGroupResponseAndHeader(1, body);
//        }
//        }
//
//class RequestResponseSerializationTest extends JUnitSuite {
//    private val leaderAndIsrRequest = SerializationTestUtils.createTestLeaderAndIsrRequest;
//    private val leaderAndIsrResponse = SerializationTestUtils.createTestLeaderAndIsrResponse;
//    private val stopReplicaRequest = SerializationTestUtils.createTestStopReplicaRequest;
//    private val stopReplicaResponse = SerializationTestUtils.createTestStopReplicaResponse;
//    private val producerRequest = SerializationTestUtils.createTestProducerRequest;
//    private val producerResponse = SerializationTestUtils.createTestProducerResponse;
//    private val fetchRequest = SerializationTestUtils.createTestFetchRequest;
//    private val offsetRequest = SerializationTestUtils.createTestOffsetRequest;
//    private val offsetResponse = SerializationTestUtils.createTestOffsetResponse;
//    private val topicMetadataRequest = SerializationTestUtils.createTestTopicMetadataRequest;
//    private val topicMetadataResponse = SerializationTestUtils.createTestTopicMetadataResponse;
//    private val offsetCommitRequestV0 = SerializationTestUtils.createTestOffsetCommitRequestV0;
//    private val offsetCommitRequestV1 = SerializationTestUtils.createTestOffsetCommitRequestV1;
//    private val offsetCommitResponse = SerializationTestUtils.createTestOffsetCommitResponse;
//    private val offsetFetchRequest = SerializationTestUtils.createTestOffsetFetchRequest;
//    private val offsetFetchResponse = SerializationTestUtils.createTestOffsetFetchResponse;
//    private val consumerMetadataRequest = SerializationTestUtils.createConsumerMetadataRequest;
//    private val consumerMetadataResponse = SerializationTestUtils.createConsumerMetadataResponse;
//    private val consumerMetadataResponseNoCoordinator = ConsumerMetadataResponse(None, ErrorMapping.ConsumerCoordinatorNotAvailableCode);
//    private val heartbeatRequest = SerializationTestUtils.createHeartbeatRequestAndHeader;
//    private val heartbeatResponse = SerializationTestUtils.createHeartbeatResponseAndHeader;
//    private val joinGroupRequest = SerializationTestUtils.createJoinGroupRequestAndHeader;
//    private val joinGroupResponse = SerializationTestUtils.createJoinGroupResponseAndHeader;
//
//    @Test
//   public void testSerializationAndDeserialization() {
//
//        val requestsAndResponses =
//                collection.immutable.Seq(leaderAndIsrRequest, leaderAndIsrResponse, stopReplicaRequest,
//                        stopReplicaResponse, producerRequest, producerResponse,
//                        fetchRequest, offsetRequest, offsetResponse, topicMetadataRequest,
//                        topicMetadataResponse, offsetCommitRequestV0, offsetCommitRequestV1,
//                        offsetCommitResponse, offsetFetchRequest, offsetFetchResponse,
//                        consumerMetadataRequest, consumerMetadataResponse,
//                        consumerMetadataResponseNoCoordinator, heartbeatRequest,
//                        heartbeatResponse, joinGroupRequest, joinGroupResponse);
//
//        requestsAndResponses.foreach { original =>
//            val buffer = ByteBuffer.allocate(original.sizeInBytes);
//            original.writeTo(buffer);
//            buffer.rewind();
//            val deserializer = original.getClass.getDeclaredMethod("readFrom", classOf<ByteBuffer>);
//            val deserialized = deserializer.invoke(null, buffer);
//            assertFalse("All serialized bytes in " + original.getClass.getSimpleName + " should have been consumed",
//                    buffer.hasRemaining);
//           Assert.assertEquals("The original and deserialized for " + original.getClass.getSimpleName + " should be the same.", original, deserialized)
//        }
//    }
}
