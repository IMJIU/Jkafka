package kafka.api;/**
 * Created by zhoulf on 2017/5/15.
 */

import kafka.common.OffsetAndMetadata;
import kafka.func.Tuple;
import kafka.log.TopicAndPartition;
import kafka.utils.Prediction;
import kafka.utils.Time;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author
 * @create 2017-05-15 00 18
 **/
public class OffsetCommitRequest extends RequestOrResponse{
    @Override
    public Integer sizeInBytes() {
        return 0;
    }

    @Override
    public void writeTo(ByteBuffer buffer) {

    }

    @Override
    public String describe(Boolean details) {
        return null;
    }
//    public static Short CurrentVersion = 1;
//    public static String DefaultClientId = "";
//    public String groupId;
//    public Map<TopicAndPartition, OffsetAndMetadata> requestInfo;
//    public Short versionId ;
//    public Integer correlationId ;
//    public String clientId ;
//    public Integer groupGenerationId = org.apache.kafka.common.requests.OffsetCommitRequest.DEFAULT_GENERATION_ID;
//    public String consumerId =  org.apache.kafka.common.requests.OffsetCommitRequest.DEFAULT_CONSUMER_ID;
//
//    public OffsetCommitRequest(java.lang.String groupId, Map<TopicAndPartition, OffsetAndMetadata> requestInfo, Short versionId, Integer correlationId, java.lang.String clientId, Integer groupGenerationId, java.lang.String consumerId) {
//        this.groupId = groupId;
//        this.requestInfo = requestInfo;
//        this.versionId = versionId;
//        this.correlationId = correlationId;
//        this.clientId = clientId;
//        this.groupGenerationId = groupGenerationId;
//        this.consumerId = consumerId;
//    }
//
//    public static OffsetCommitRequest   readFrom(ByteBuffer buffer) {
//        // Read values from the envelope;
//       Short versionId = buffer.getShort();
//       Prediction.Assert(versionId == 0 || versionId == 1,
//        "Version " + versionId + " is invalid for OffsetCommitRequest. Valid versions are 0 or 1.");
//
//        Integer correlationId = buffer.getInt();
//        String clientId = ApiUtils.readShortString(buffer);
//
//        // Read the OffsetRequest;
//       String consumerGroupId = ApiUtils.readShortString(buffer);
//
//        // version 1 specific fields;
//         Integer groupGenerationId = org.apache.kafka.common.requests.OffsetCommitRequest.DEFAULT_GENERATION_ID;
//         String consumerId = org.apache.kafka.common.requests.OffsetCommitRequest.DEFAULT_CONSUMER_ID;
//        if (versionId == 1) {
//            groupGenerationId = buffer.getInt();
//            consumerId = ApiUtils.readShortString(buffer);
//        }
//
//       Integer topicCount = buffer.getInt();
//       pairs = Stream.iterate(1,n->n+1).limit(topicCount).flatMap(n->{
//           String topic = ApiUtils.readShortString(buffer);
//           Integer partitionCount = buffer.getInt();
//           Stream.iterate(1,m->m+1).limit(partitionCount).map(m->{
//               Integer partitionId = buffer.getInt();
//               Long offset = buffer.getLong();
//               Long timestamp ;
//               if (versionId == 1) {
//                   Long given = buffer.getLong();
//                   timestamp=given;
//               } else{
//                       timestamp= OffsetAndMetadata.InvalidTime;
//                 }
//               String metadata = ApiUtils.readShortString(buffer);
//              return Tuple.of(new TopicAndPartition(topic, partitionId), new OffsetAndMetadata(offset, metadata, timestamp));
//            });
//        return new OffsetCommitRequest(consumerGroupId, immutable.Map(_ pairs*), versionId, correlationId, clientId, groupGenerationId, consumerId);
//    }
//
//   public static void changeInvalidTimeToCurrentTime(OffsetCommitRequest offsetCommitRequest) {
//        Long now = Time.get().milliseconds();
//        for ( (topicAndPartiiton, offsetAndMetadata) <- offsetCommitRequest.requestInfo)
//        if (offsetAndMetadata.timestamp == OffsetAndMetadata.InvalidTime)
//            offsetAndMetadata.timestamp = now;
//    }
//
//case class OffsetCommitRequest(String groupId,
//        immutable requestInfo.Map<TopicAndPartition, OffsetAndMetadata>,
//        Short versionId = OffsetCommitRequest.CurrentVersion,
//        Integer correlationId = 0,
//        String clientId = OffsetCommitRequest.DefaultClientId,
//        Integer groupGenerationId = org.apache.kafka.common.requests.OffsetCommitRequest.DEFAULT_GENERATION_ID,
//        String consumerId =  org.apache.kafka.common.requests.OffsetCommitRequest.DEFAULT_CONSUMER_ID);
//        extends RequestOrResponse(Some(RequestKeys.OffsetCommitKey)) {
//        assert(versionId == 0 || versionId == 1,
//        "Version " + versionId + " is invalid for OffsetCommitRequest. Valid versions are 0 or 1.")
//
//        lazy val requestInfoGroupedByTopic = requestInfo.groupBy(_._1.topic);
//
//       public void filterLargeMetadata(Int maxMetadataSize) =
//        requestInfo.filter(info => info._2.metadata == null || info._2.metadata.length <= maxMetadataSize);
//
//       public void responseFor(Short errorCode, Int offsetMetadataMaxSize) = {
//        val commitStatus = requestInfo.map {info =>
//        (info._1, if (info._2.metadata != null && info._2.metadata.length > offsetMetadataMaxSize)
//        ErrorMapping.OffsetMetadataTooLargeCode;
//        else if (errorCode == ErrorMapping.UnknownTopicOrPartitionCode)
//        ErrorMapping.ConsumerCoordinatorNotAvailableCode;
//        else if (errorCode == ErrorMapping.NotLeaderForPartitionCode)
//        ErrorMapping.NotCoordinatorForConsumerCode;
//        else;
//        errorCode);
//        }.toMap;
//        OffsetCommitResponse(commitStatus, correlationId);
//        }
//
//       public void writeTo(ByteBuffer buffer) {
//        // Write envelope;
//        buffer.putShort(versionId);
//        buffer.putInt(correlationId);
//        writeShortString(buffer, clientId);
//
//        // Write OffsetCommitRequest;
//        writeShortString(buffer, groupId)             // consumer group;
//
//        // version 1 specific data;
//        if (versionId == 1) {
//        buffer.putInt(groupGenerationId);
//        writeShortString(buffer, consumerId);
//        }
//        buffer.putInt(requestInfoGroupedByTopic.size) // number of topics;
//        requestInfoGroupedByTopic.foreach( t1 => { // topic -> Map<TopicAndPartition, OffsetMetadataAndError>
//        writeShortString(buffer, t1._1) // topic;
//        buffer.putInt(t1._2.size)       // number of partitions for this topic;
//        t1._2.foreach( t2 => {
//        buffer.putInt(t2._1.partition);
//        buffer.putLong(t2._2.offset);
//        if (versionId == 1)
//        buffer.putLong(t2._2.timestamp);
//        writeShortString(buffer, t2._2.metadata);
//        });
//        });
//        }
//
//        overridepublic void sizeInBytes =
//        2 + /* versionId */
//        4 + /* correlationId */
//        shortStringLength(clientId) +
//        shortStringLength(groupId) +
//        (if (versionId == 1) 4 /* group generation id */ + shortStringLength(consumerId) else 0) +
//        4 + /* topic count */
//        requestInfoGroupedByTopic.foldLeft(0)((count, topicAndOffsets) => {
//        val (topic, offsets) = topicAndOffsets;
//        count +
//        shortStringLength(topic) + /* topic */
//        4 + /* number of partitions */
//        offsets.foldLeft(0)((innerCount, offsetAndMetadata) => {
//        innerCount +
//        4 /* partition */ +
//        8 /* offset */ +
//        (if (versionId == 1) 8 else 0 ) /* timestamp */ +
//        shortStringLength(offsetAndMetadata._2.metadata);
//        });
//        });
//
//        override public Unit  void handleError(Throwable e, RequestChannel requestChannel, RequestChannel request.Request) {
//        val errorCode = ErrorMapping.codeFor(e.getClass.asInstanceOf<Class<Throwable>>);
//        val errorResponse = responseFor(errorCode, Integer.MAX_VALUE);
//        requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend(errorResponse)));
//        }
//
//        overridepublic String  void describe(Boolean details) {
//        val offsetCommitRequest = new StringBuilder;
//        offsetCommitRequest.append("Name: " + this.getClass.getSimpleName);
//        offsetCommitRequest.append("; Version: " + versionId);
//        offsetCommitRequest.append("; CorrelationId: " + correlationId);
//        offsetCommitRequest.append("; ClientId: " + clientId);
//        offsetCommitRequest.append("; GroupId: " + groupId);
//        offsetCommitRequest.append("; GroupGenerationId: " + groupGenerationId);
//        offsetCommitRequest.append("; ConsumerId: " + consumerId);
//        if(details)
//        offsetCommitRequest.append("; RequestInfo: " + requestInfo.mkString(","));
//        offsetCommitRequest.toString();
//        }
//
//        overridepublic void toString = {
//        describe(details = true);
//        }
}
