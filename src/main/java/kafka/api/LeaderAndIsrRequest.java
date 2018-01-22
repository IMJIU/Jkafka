package kafka.api;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.func.Handler;
import kafka.func.IntCount;
import kafka.func.Tuple;
import kafka.network.BoundedByteBufferSend;
import kafka.network.RequestChannel;
import kafka.utils.Utils;
import org.apache.zookeeper.Op;

import java.nio.ByteBuffer;
import java.util.*;

import static kafka.api.ApiUtils.*;

/**
 * @author zhoulf
 * @create 2017-10-20 55 10
 **/

public class LeaderAndIsrRequest extends RequestOrResponse {
    public static final short CurrentVersion = 0;
    public static final Boolean IsInit = true;
    public static final Boolean NotInit = false;
    public static final Integer DefaultAckTimeout = 1000;
    public Short versionId;
    public Integer correlationId;
    public String clientId;
    public Integer controllerId;
    public Integer controllerEpoch;
    public Map<Tuple<String, Integer>, PartitionStateInfo> partitionStateInfos;
    public Set<Broker> leaders;

    public LeaderAndIsrRequest(Short versionId, Integer correlationId, String clientId, Integer controllerId, Integer controllerEpoch, Map<Tuple<String, Integer>, PartitionStateInfo> partitionStateInfos, Set<Broker> leaders) {
        super(Optional.of(RequestKeys.LeaderAndIsrKey));
        this.versionId = versionId;
        this.correlationId = correlationId;
        this.clientId = clientId;
        this.controllerId = controllerId;
        this.controllerEpoch = controllerEpoch;
        this.partitionStateInfos = partitionStateInfos;
        this.leaders = leaders;
    }

    public LeaderAndIsrRequest(Map<Tuple<String, Integer>, PartitionStateInfo> partitionStateInfos, Set<Broker> leaders,
                               Integer controllerId, Integer controllerEpoch, Integer correlationId, String clientId) {
        this(LeaderAndIsrRequest.CurrentVersion, correlationId, clientId, controllerId, controllerEpoch, partitionStateInfos, leaders);
    }

    public final static Handler<ByteBuffer, LeaderAndIsrRequest> readFrom = (buffer) -> {
        short versionId = buffer.getShort();
        int correlationId = buffer.getInt();
        String clientId = readShortString(buffer);
        int controllerId = buffer.getInt();
        int controllerEpoch = buffer.getInt();
        int partitionStateInfosCount = buffer.getInt();
        Map<Tuple<String, Integer>, PartitionStateInfo> partitionStateInfos = Maps.newHashMap();

        for (int i = 0; i < partitionStateInfosCount; i++) {
            String topic = readShortString(buffer);
            int partition = buffer.getInt();
            PartitionStateInfo partitionStateInfo = PartitionStateInfo.readFrom(buffer);

            partitionStateInfos.put(Tuple.of(topic, partition), partitionStateInfo);
        }

        int leadersCount = buffer.getInt();
        Set<Broker> leaders = Sets.newHashSet();
        for (int i = 0; i < leadersCount; i++)
            leaders.add(Broker.readFrom(buffer));

        return new LeaderAndIsrRequest(versionId, correlationId, clientId, controllerId, controllerEpoch, partitionStateInfos, leaders);
    };


    public void writeTo(ByteBuffer buffer) {
        buffer.putShort(versionId);
        buffer.putInt(correlationId);
        writeShortString(buffer, clientId);
        buffer.putInt(controllerId);
        buffer.putInt(controllerEpoch);
        buffer.putInt(partitionStateInfos.size());
        Utils.foreach(partitionStateInfos, (key, value) -> {
            writeShortString(buffer, key.v1);
            buffer.putInt(key.v2);
            value.writeTo(buffer);
        });
        buffer.putInt(leaders.size());
        leaders.forEach(l -> l.writeTo(buffer));
    }

    public Integer sizeInBytes() {
        IntCount size = IntCount.of(
                2 /* version id */ +
                        4 /* correlation id */ +
                        (2 + clientId.length()) /* client id */ +
                        4 /* controller id */ +
                        4 /* controller epoch */ +
                        4 /* number of partitions */);
        Utils.foreach(partitionStateInfos, (key, value) ->
                size.add(2 + key.v1.length() /* topic */ + 4 /* partition */ + value.sizeInBytes()) /* partition state info */
        );
        size.add(4); /* number of leader brokers */
        for (Broker broker : leaders)
            size.add(broker.sizeInBytes()); /* broker info */
        return size.get();
    }

    @Override
    public String toString() {
        return describe(true);
    }

    @Override
    public void handleError(Throwable e, RequestChannel requestChannel, RequestChannel.Request request) {
        Map<Tuple<String, Integer>, Short> responseMap = Utils.toMap(
                Utils.map(partitionStateInfos, (topicAndPartition, partitionAndState) ->
                        Tuple.of(topicAndPartition, ErrorMapping.codeFor(e.getClass()))));
        LeaderAndIsrResponse errorResponse = new LeaderAndIsrResponse(correlationId, responseMap);
        requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(errorResponse)));
    }

    @Override
    public String describe(Boolean details) {
        StringBuilder leaderAndIsrRequest = new StringBuilder();
        leaderAndIsrRequest.append("Name:" + this.getClass().getSimpleName());
        leaderAndIsrRequest.append(";Version:" + versionId);
        leaderAndIsrRequest.append(";Controller:" + controllerId);
        leaderAndIsrRequest.append(";ControllerEpoch:" + controllerEpoch);
        leaderAndIsrRequest.append(";CorrelationId:" + correlationId);
        leaderAndIsrRequest.append(";ClientId:" + clientId);
        leaderAndIsrRequest.append(";Leaders:" + leaders);
        if (details)
            leaderAndIsrRequest.append(";PartitionState:" + partitionStateInfos);
        return leaderAndIsrRequest.toString();
    }
}
