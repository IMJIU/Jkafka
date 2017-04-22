package kafka.api;

import kafka.func.Handler;

import java.nio.ByteBuffer;

/**
 * Created by Administrator on 2017/4/22.
 */
public class ProducerRequest {
        public final static Short CurrentVersion = 0;

       public final static Handler<ByteBuffer,ProducerRequest> readFrom=(buffer)->{
//                 Short versionId = buffer.getShort();
//         Integer correlationId = buffer.getInt();
//         String clientId = readShortString(buffer);
//         Short requiredAcks = buffer.getShort();
//         Integer ackTimeoutMs = buffer.getInt();
//        //build the topic structure;
//           Integer topicCount = buffer.getInt();
//         partitionDataPairs = (1 to topicCount).flatMap(_ => {
//                // process topic;
//                val topic = readShortString(buffer);
//                val partitionCount = buffer.getInt;
//                (1 to partitionCount).map(_ => {
//                val partition = buffer.getInt;
//                val messageSetSize = buffer.getInt;
//                val messageSetBuffer = new Array<Byte>(messageSetSize);
//                buffer.get(messageSetBuffer,0,messageSetSize);
//                (TopicAndPartition(topic, partition), new ByteBufferMessageSet(ByteBuffer.wrap(messageSetBuffer)));
//        });
//        });
//
//        ProducerRequest(versionId, correlationId, clientId, requiredAcks, ackTimeoutMs, collection.mutable.Map(_ partitionDataPairs*));
//        }
           return null;
    };

//    case class ProducerRequest(Short versionId = ProducerRequest.CurrentVersion,
//    Int correlationId,
//    String clientId,
//    Short requiredAcks,
//    Int ackTimeoutMs,
//    collection data.mutable.Map<TopicAndPartition, ByteBufferMessageSet>);
//            extends RequestOrResponse(Some(RequestKeys.ProduceKey)) {
//
//        /**
//         * Partitions the data into a map of maps (one for each topic).
//         */
//        private lazy val dataGroupedByTopic = data.groupBy(_._1.topic);
//        val topicPartitionMessageSizeMap = data.map(r => r._1 -> r._2.sizeInBytes).toMap;
//
//       public void this(Int correlationId,
//                String clientId,
//                Short requiredAcks,
//                Int ackTimeoutMs,
//                collection data.mutable.Map<TopicAndPartition, ByteBufferMessageSet>) =
//        this(ProducerRequest.CurrentVersion, correlationId, clientId, requiredAcks, ackTimeoutMs, data);
//
//       public void writeTo(ByteBuffer buffer) {
//            buffer.putShort(versionId);
//            buffer.putInt(correlationId);
//            writeShortString(buffer, clientId);
//            buffer.putShort(requiredAcks);
//            buffer.putInt(ackTimeoutMs);
//
//            //save the topic structure;
//            buffer.putInt(dataGroupedByTopic.size) //the number of topics;
//            dataGroupedByTopic.foreach {
//                case (topic, topicAndPartitionData) =>
//                    writeShortString(buffer, topic) //write the topic;
//                    buffer.putInt(topicAndPartitionData.size) //the number of partitions;
//                    topicAndPartitionData.foreach(partitionAndData => {
//                            val partition = partitionAndData._1.partition;
//                            val partitionMessageData = partitionAndData._2;
//                            val bytes = partitionMessageData.buffer;
//                            buffer.putInt(partition);
//                            buffer.putInt(bytes.limit);
//                            buffer.put(bytes);
//                            bytes.rewind;
//                    });
//            }
//        }
//
//       public void Integer sizeInBytes = {
//                2 + /* versionId */
//                        4 + /* correlationId */
//                        shortStringLength(clientId) + /* client id */
//                        2 + /* requiredAcks */
//                        4 + /* ackTimeoutMs */
//                        4 + /* number of topics */
//                        dataGroupedByTopic.foldLeft(0)((foldedTopics, currTopic) => {
//            foldedTopics +
//                    shortStringLength(currTopic._1) +
//                    4 + /* the number of partitions */
//                    {
//                            currTopic._2.foldLeft(0)((foldedPartitions, currPartition) => {
//                foldedPartitions +
//                        4 + /* partition id */
//                        4 + /* byte-length of serialized messages */
//                        currPartition._2.sizeInBytes;
//            });
//            }
//        });
//        }
//
//       public void numPartitions = data.size;
//
//        overridepublic String  void toString() {
//                describe(true);
//        }
//
//        override public Unit  void handleError(Throwable e, RequestChannel requestChannel, RequestChannel request.Request) {
//        if(request.requestObj.asInstanceOf<ProducerRequest>.requiredAcks == 0) {
//            requestChannel.closeConnection(request.processor, request);
//        }
//        else {
//            val producerResponseStatus = data.map {
//                case (topicAndPartition, data) =>
//                    (topicAndPartition, ProducerResponseStatus(ErrorMapping.codeFor(e.getClass.asInstanceOf<Class[Throwable]>), -1l));
//            }
//            val errorResponse = ProducerResponse(correlationId, producerResponseStatus);
//            requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend(errorResponse)));
//        }
//        }
//
//        overridepublic String  void describe(Boolean details) {
//                val producerRequest = new StringBuilder;
//                producerRequest.append("Name: " + this.getClass.getSimpleName);
//                producerRequest.append("; Version: " + versionId);
//                producerRequest.append("; CorrelationId: " + correlationId);
//                producerRequest.append("; ClientId: " + clientId);
//                producerRequest.append("; RequiredAcks: " + requiredAcks);
//                producerRequest.append("; AckTimeoutMs: " + ackTimeoutMs + " ms");
//        if(details)
//            producerRequest.append("; TopicAndPartition: " + topicPartitionMessageSizeMap.mkString(","));
//        producerRequest.toString();
//        }
//
//
//       public void emptyData(){
//            data.clear();
//        }
//    }
}
