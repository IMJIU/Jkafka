package kafka.api;

import com.google.common.collect.Maps;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.func.IntCount;
import kafka.log.TopicAndPartition;
import kafka.network.BoundedByteBufferSend;
import kafka.network.RequestChannel;
import kafka.utils.Utils;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static kafka.api.ApiUtils.*;

/**
 * @author zhoulf
 * @create 2017-10-19 16 9
 **/

public class UpdateMetadataRequest extends RequestOrResponse {
    public static Short CurrentVersion = 0;
    public static Boolean IsInit = true;
    public static Boolean NotInit = false;
    public static Integer DefaultAckTimeout = 1000;

    public Short versionId;
    public Integer correlationId;
    public String clientId;
    public Integer controllerId;
    public Integer controllerEpoch;
    public Map<TopicAndPartition, PartitionStateInfo> partitionStateInfos;
    public Set<Broker> aliveBrokers;

    public UpdateMetadataRequest(Short versionId, Integer correlationId, String clientId, Integer controllerId, Integer controllerEpoch, Map<TopicAndPartition, PartitionStateInfo> partitionStateInfos, Set<Broker> aliveBrokers) {
        this.versionId = versionId;
        this.correlationId = correlationId;
        this.clientId = clientId;
        this.controllerId = controllerId;
        this.controllerEpoch = controllerEpoch;
        this.partitionStateInfos = partitionStateInfos;
        this.aliveBrokers = aliveBrokers;
    }

    public UpdateMetadataRequest readFrom(ByteBuffer buffer) {
        short versionId = buffer.getShort();
        int correlationId = buffer.getInt();
        String clientId = readShortString(buffer);
        int controllerId = buffer.getInt();
        int controllerEpoch = buffer.getInt();
        int partitionStateInfosCount = buffer.getInt();
        Map<TopicAndPartition, PartitionStateInfo> partitionStateInfos = Maps.newHashMap();

        for (int i = 0; i < partitionStateInfosCount; i++) {
            String topic = readShortString(buffer);
            Integer partition = buffer.getInt();
            PartitionStateInfo partitionStateInfo = PartitionStateInfo.readFrom(buffer);

            partitionStateInfos.put(new TopicAndPartition(topic, partition), partitionStateInfo);
        }

        int numAliveBrokers = buffer.getInt();
        Set<Broker> aliveBrokers = Utils.yieldSet(0, numAliveBrokers, () -> Broker.readFrom(buffer));
        return new UpdateMetadataRequest(versionId, correlationId, clientId, controllerId, controllerEpoch,
                partitionStateInfos, aliveBrokers);
    }


//       ){

    public UpdateMetadataRequest(Integer controllerId, Integer controllerEpoch, Integer correlationId, String clientId,
                                 Map<TopicAndPartition, PartitionStateInfo> partitionStateInfos,
                                 Set<Broker> aliveBrokers) {
        this(UpdateMetadataRequest.CurrentVersion, correlationId, clientId,
                controllerId, controllerEpoch, partitionStateInfos, aliveBrokers);
    }

    public void writeTo(ByteBuffer buffer) {
        buffer.putShort(versionId);
        buffer.putInt(correlationId);
        writeShortString(buffer, clientId);
        buffer.putInt(controllerId);
        buffer.putInt(controllerEpoch);
        buffer.putInt(partitionStateInfos.size());
        Utils.foreach(partitionStateInfos, (key, value) -> {
            writeShortString(buffer, key.topic);
            buffer.putInt(key.partition);
            value.writeTo(buffer);
        });
        buffer.putInt(aliveBrokers.size());
        aliveBrokers.forEach(b -> b.writeTo(buffer));
    }

    public Integer sizeInBytes() {
        final IntCount size = IntCount.of(
                2 /* version id */ +
                        4 /* correlation id */ +
                        (2 + clientId.length()) /* client id */ +
                        4 /* controller id */ +
                        4 /* controller epoch */ +
                        4 /* number of partitions */);
        Utils.foreach(partitionStateInfos, (key, value) ->
                size.add(2 + key.topic.length()/* topic */ + 4 /* partition */ + value.sizeInBytes()) /* partition state info */
        );
        size.add(4) /* number of alive brokers in the cluster */;
        for (Broker broker : aliveBrokers)
            size.add(broker.sizeInBytes()); /* broker info */
        return size.get();
    }

    @Override
    public String toString() {
        return describe(true);
    }

    @Override
    public void handleError(Throwable e, RequestChannel requestChannel, RequestChannel.Request request) {
        UpdateMetadataResponse errorResponse = new UpdateMetadataResponse(correlationId, ErrorMapping.codeFor(e.getCause().getClass()));
        requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(errorResponse)));
    }

    @Override
    public String describe(Boolean details) {
        StringBuilder updateMetadataRequest = new StringBuilder();
        updateMetadataRequest.append("Name:" + this.getClass().getSimpleName());
        updateMetadataRequest.append(";Version:" + versionId);
        updateMetadataRequest.append(";Controller:" + controllerId);
        updateMetadataRequest.append(";ControllerEpoch:" + controllerEpoch);
        updateMetadataRequest.append(";CorrelationId:" + correlationId);
        updateMetadataRequest.append(";ClientId:" + clientId);
        updateMetadataRequest.append(";AliveBrokers:" + aliveBrokers);
        if (details)
            updateMetadataRequest.append(";PartitionState:" + partitionStateInfos);
        return updateMetadataRequest.toString();
    }
}
