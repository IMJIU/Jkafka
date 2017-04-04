package kafka.api;

/**
 * Created by Administrator on 2017/4/4.
 */
public class OffsetRequest {
    public static final Short CurrentVersion = 0;
    public static final String DefaultClientId = "";

    public static final String SmallestTimeString = "smallest";
    public static final String LargestTimeString = "largest";
    public static final Long LatestTime = -1L;
    public static final Long EarliestTime = -2L;

//        def readFrom(buffer: ByteBuffer): OffsetRequest = {
//                val versionId = buffer.getShort
//                val correlationId = buffer.getInt
//                val clientId = readShortString(buffer)
//                val replicaId = buffer.getInt
//                val topicCount = buffer.getInt
//                val pairs = (1 to topicCount).flatMap(_ => {
//                val topic = readShortString(buffer)
//                val partitionCount = buffer.getInt
//                (1 to partitionCount).map(_ => {
//                val partitionId = buffer.getInt
//                val time = buffer.getLong
//                val maxNumOffsets = buffer.getInt
//                (TopicAndPartition(topic, partitionId), PartitionOffsetRequestInfo(time, maxNumOffsets))
//        })
//        })
//        OffsetRequest(Map(pairs:_*), versionId= versionId, clientId = clientId, correlationId = correlationId, replicaId = replicaId)
//        }
//    }

//    case class OffsetRequest(requestInfo: Map[TopicAndPartition, PartitionOffsetRequestInfo],
//    versionId: Short = OffsetRequest.CurrentVersion,
//    correlationId: Int = 0,
//    clientId: String = OffsetRequest.DefaultClientId,
//    replicaId: Int = Request.OrdinaryConsumerId)
//            extends RequestOrResponse(Some(RequestKeys.OffsetsKey)) {
//
//        def this(requestInfo: Map[TopicAndPartition, PartitionOffsetRequestInfo], correlationId: Int, replicaId: Int) = this(requestInfo, OffsetRequest.CurrentVersion, correlationId, OffsetRequest.DefaultClientId, replicaId)
//
//        lazy val requestInfoGroupedByTopic = requestInfo.groupBy(_._1.topic)
//
//        def writeTo(buffer: ByteBuffer) {
//            buffer.putShort(versionId)
//            buffer.putInt(correlationId)
//            writeShortString(buffer, clientId)
//            buffer.putInt(replicaId)
//
//            buffer.putInt(requestInfoGroupedByTopic.size) // topic count
//            requestInfoGroupedByTopic.foreach {
//                case((topic, partitionInfos)) =>
//                    writeShortString(buffer, topic)
//                    buffer.putInt(partitionInfos.size) // partition count
//                    partitionInfos.foreach {
//                    case (TopicAndPartition(_, partition), partitionInfo) =>
//                        buffer.putInt(partition)
//                        buffer.putLong(partitionInfo.time)
//                        buffer.putInt(partitionInfo.maxNumOffsets)
//                }
//            }
//        }
//
//        def sizeInBytes =
//                2 + /* versionId */
//                        4 + /* correlationId */
//                        shortStringLength(clientId) +
//                        4 + /* replicaId */
//                        4 + /* topic count */
//                        requestInfoGroupedByTopic.foldLeft(0)((foldedTopics, currTopic) => {
//            val (topic, partitionInfos) = currTopic
//            foldedTopics +
//                    shortStringLength(topic) +
//                    4 + /* partition count */
//                    partitionInfos.size * (
//                            4 + /* partition */
//                                    8 + /* time */
//                                    4 /* maxNumOffsets */
//                    )
//        })
//
//        def isFromOrdinaryClient = replicaId == Request.OrdinaryConsumerId
//        def isFromDebuggingClient = replicaId == Request.DebuggingConsumerId
//
//        override def toString(): String = {
//                describe(true)
//        }
//
//        override  def handleError(e: Throwable, requestChannel: RequestChannel, request: RequestChannel.Request): Unit = {
//                val partitionOffsetResponseMap = requestInfo.map {
//        case (topicAndPartition, partitionOffsetRequest) =>
//            (topicAndPartition, PartitionOffsetsResponse(ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]), null))
//            }
//            val errorResponse = OffsetResponse(correlationId, partitionOffsetResponseMap)
//            requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend(errorResponse)))
//            }
//
//            override def describe(details: Boolean): String = {
//                val offsetRequest = new StringBuilder
//                offsetRequest.append("Name: " + this.getClass.getSimpleName)
//                offsetRequest.append("; Version: " + versionId)
//                offsetRequest.append("; CorrelationId: " + correlationId)
//                offsetRequest.append("; ClientId: " + clientId)
//                offsetRequest.append("; ReplicaId: " + replicaId)
//            if(details)
//                offsetRequest.append("; RequestInfo: " + requestInfo.mkString(","))
//            offsetRequest.toString()
//            }
//    }
}
