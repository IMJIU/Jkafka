package kafka.api;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.func.Tuple;
import kafka.utils.Logging;
import kafka.utils.Sc;
import kafka.utils.Utils;
import java.nio.ByteBuffer;
import java.util.*;
import static kafka.api.ApiUtils.*;

/**
 * @author zhoulf
 * @create 2017-10-19 07 17
 **/

public class PartitionMetadata extends Logging {
    public Integer partitionId;
    public Broker leader;
    public  List<Broker> replicas;
    public  List<Broker> isr = Lists.newArrayList();
    public  Short errorCode = ErrorMapping.NoError;

    public PartitionMetadata(java.lang.Integer partitionId, Broker leader, List<Broker> replicas, List<Broker> isr, Short errorCode) {
        this.partitionId = partitionId;
        this.leader = leader;
        this.replicas = replicas;
        this.isr = isr;
        this.errorCode = errorCode;
    }

    public static PartitionMetadata readFrom(ByteBuffer buffer, Map<Integer, Broker> brokers) {
        short errorCode = readShortInRange(buffer, "error code", Tuple.of((short) -1, Short.MAX_VALUE));
        int partitionId = readIntInRange(buffer, "partition id", Tuple.of(0, Integer.MAX_VALUE)); /* partition id */
        int leaderId = buffer.getInt();
        Broker leader = brokers.get(leaderId);

    /* list of all replicas */
        int numReplicas = readIntInRange(buffer, "number of all replicas", Tuple.of(0, Integer.MAX_VALUE));
        List<Integer> replicaIds = Sc.itToList(0, numReplicas, n -> buffer.getInt());
        List<Broker> replicas = Sc.map(replicaIds, r -> brokers.get(r));

    /* list of in-sync replicas */
        int numIsr = readIntInRange(buffer, "number of in-sync replicas", Tuple.of(0, Integer.MAX_VALUE));
        List<Integer> isrIds = Sc.itToList(0, numIsr, n -> buffer.getInt());
        List<Broker> isr = Sc.map(isrIds, r -> brokers.get(r));
        return new PartitionMetadata(partitionId, leader, replicas, isr, errorCode);
    }


    public Integer sizeInBytes() {
        return 2 /* error code */ +
                4 /* partition id */ +
                4 /* leader */ +
                4 + 4 * replicas.size() /* replica array */ +
                4 + 4 * isr.size(); /* isr array */
    }

    public void writeTo(ByteBuffer buffer) {
        buffer.putShort(errorCode);
        buffer.putInt(partitionId);

    /* leader */
        Integer leaderId = leader != null ? leader.id : TopicMetadata.NoLeaderNodeId;
        buffer.putInt(leaderId);

    /* number of replicas */
        buffer.putInt(replicas.size());
        replicas.forEach(r -> buffer.putInt(r.id));

    /* number of in-sync replicas */
        buffer.putInt(isr.size());
        isr.forEach(r -> buffer.putInt(r.id));
    }

    @Override
    public String toString() {
        StringBuilder partitionMetadataString = new StringBuilder();
        partitionMetadataString.append("\tpartition " + partitionId);
        partitionMetadataString.append("\tleader: " + (leader!=null ?formatBroker(leader) : "none"));
        partitionMetadataString.append("\treplicas: " + Sc.map(replicas, this::formatBroker));
        partitionMetadataString.append("\tisr: " + Sc.map(isr, this::formatBroker));
        partitionMetadataString.append(String.format("\tisUnderReplicated: %s", isr.size() < replicas.size() ? "true" : "false"));
        return partitionMetadataString.toString();
    }

    private String formatBroker(Broker broker) {
        return broker.id + " (" + org.apache.kafka.common.utils.Utils.formatAddress(broker.host, broker.port) + ")";
    }
}
