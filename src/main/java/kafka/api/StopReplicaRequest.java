package kafka.api;

import com.google.common.collect.Sets;
import kafka.common.ErrorMapping;
import kafka.func.IntCount;
import kafka.func.Tuple;
import kafka.log.TopicAndPartition;
import kafka.network.BoundedByteBufferSend;
import kafka.network.InvalidRequestException;
import kafka.network.RequestChannel;
import kafka.utils.Logging;
import kafka.utils.Utils;

import static kafka.api.ApiUtils.*;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;

/**
 * @author zhoulf
 * @create 2017-10-12 12 15
 **/

public class StopReplicaRequest extends RequestOrResponse {
    public static final short CurrentVersion = 0;
    public static final String DefaultClientId = "";
    public static final int DefaultAckTimeout = 100;
    public Short versionId;
    public Integer correlationId;
    public String clientId;
    public Integer controllerId;
    public Integer controllerEpoch;
    public Boolean deletePartitions;
    public Set<TopicAndPartition> partitions;

    public StopReplicaRequest(Short versionId, Integer correlationId, String clientId, Integer controllerId, Integer controllerEpoch, Boolean deletePartitions, Set<TopicAndPartition> partitions) {
        this.versionId = versionId;
        this.correlationId = correlationId;
        this.clientId = clientId;
        this.controllerId = controllerId;
        this.controllerEpoch = controllerEpoch;
        this.deletePartitions = deletePartitions;
        this.partitions = partitions;
    }

    public StopReplicaRequest readFrom(ByteBuffer buffer) {
        short versionId = buffer.getShort();
        int correlationId = buffer.getInt();
        String clientId = readShortString(buffer);
        int controllerId = buffer.getInt();
        int controllerEpoch = buffer.getInt();
        byte n = buffer.get();

        boolean deletePartitions;
        if (n == 1) deletePartitions = true;
        else if (n == 0) deletePartitions = false;
        else
            throw new InvalidRequestException(String.format("Invalid byte %d in delete partitions field. (Assuming false.)", n));

        int topicPartitionPairCount = buffer.getInt();
        Set<TopicAndPartition> topicPartitionPairSet = Sets.newHashSet();
        Utils.it(1, topicPartitionPairCount, m ->
                topicPartitionPairSet.add(new TopicAndPartition(readShortString(buffer), buffer.getInt())));
        return new StopReplicaRequest(versionId, correlationId, clientId, controllerId, controllerEpoch, deletePartitions, topicPartitionPairSet);
    }


    public StopReplicaRequest(Boolean deletePartitions, Set<TopicAndPartition> partitions, Integer controllerId, Integer controllerEpoch, Integer correlationId) {
        this(StopReplicaRequest.CurrentVersion, correlationId, StopReplicaRequest.DefaultClientId,
                controllerId, controllerEpoch, deletePartitions, partitions);
    }

    public void writeTo(ByteBuffer buffer) {
        buffer.putShort(versionId);
        buffer.putInt(correlationId);
        writeShortString(buffer, clientId);
        buffer.putInt(controllerId);
        buffer.putInt(controllerEpoch);
        buffer.put(deletePartitions ? (byte) 1 : (byte) 0);
        buffer.putInt(partitions.size());
        for (TopicAndPartition topicAndPartition : partitions) {
            writeShortString(buffer, topicAndPartition.topic);
            buffer.putInt(topicAndPartition.partition);
        }
    }

    public Integer sizeInBytes() {
        IntCount size = IntCount.of(0);
        size.add(
                2 + /* versionId */
                        4 + /* correlation id */
                        ApiUtils.shortStringLength(clientId) +
                        4 + /* controller id*/
                        4 + /* controller epoch */
                        1 + /* deletePartitions */
                        4); /* partition count */
        for (TopicAndPartition topicAndPartition : partitions) {
            size.add(ApiUtils.shortStringLength(topicAndPartition.topic) + 4/* partition id */);
        }
        return size.get();
    }

    @Override
    public String toString() {
        return describe(true);
    }

    @Override
    public void handleError(Throwable e, RequestChannel requestChannel, RequestChannel.Request request) {
        Map<TopicAndPartition, Short> responseMap = Utils.toMap(Utils.map(partitions, p -> Tuple.of(p, ErrorMapping.NoError)));
        StopReplicaResponse errorResponse = new StopReplicaResponse(correlationId, responseMap);
        requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(errorResponse)));
    }

    @Override
    public String describe(Boolean details) {
        StringBuilder stopReplicaRequest = new StringBuilder();
        stopReplicaRequest.append("Name: " + this.getClass().getSimpleName());
        stopReplicaRequest.append("; Version: " + versionId);
        stopReplicaRequest.append("; CorrelationId: " + correlationId);
        stopReplicaRequest.append("; ClientId: " + clientId);
        stopReplicaRequest.append("; DeletePartitions: " + deletePartitions);
        stopReplicaRequest.append("; ControllerId: " + controllerId);
        stopReplicaRequest.append("; ControllerEpoch: " + controllerEpoch);
        if (details)
            stopReplicaRequest.append("; Partitions: " + partitions);
        stopReplicaRequest.toString();
        return stopReplicaRequest.toString();
    }
}
