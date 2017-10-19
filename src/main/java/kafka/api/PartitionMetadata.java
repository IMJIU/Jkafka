package kafka.api;

import com.google.common.collect.Sets;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.utils.Logging;
import kafka.utils.Utils;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.Optional;

/**
 * @author zhoulf
 * @create 2017-10-19 07 17
 **/

public class PartitionMetadata extends Logging {
    Integer partitionId;
    Optional<Broker> leader;
    List<Broker> replicas;
    Set<Broker> isr = Sets.newHashSet();
    Short errorCode = ErrorMapping.NoError;

    public PartitionMetadata(java.lang.Integer partitionId, Optional<Broker> leader, List<Broker> replicas, Set<Broker> isr, Short errorCode) {
        this.partitionId = partitionId;
        this.leader = leader;
        this.replicas = replicas;
        this.isr = isr;
        this.errorCode = errorCode;
    }

    public static PartitionMetadata readFrom(ByteBuffer buffer, Map<Integer, Broker> brokers>) {
        val errorCode = readShortInRange(buffer, "error code", (-1, Short.MaxValue));
        val partitionId = readIntInRange(buffer, "partition id", (0, Integer.MAX_VALUE)) /* partition id */
        val leaderId = buffer.getInt;
        val leader = brokers.get(leaderId);

    /* list of all replicas */
        val numReplicas = readIntInRange(buffer, "number of all replicas", (0, Integer.MAX_VALUE));
        val replicaIds = (0 until numReplicas).map(_ = > buffer.getInt);
        val replicas = replicaIds.map(brokers);

    /* list of in-sync replicas */
        val numIsr = readIntInRange(buffer, "number of in-sync replicas", (0, Integer.MAX_VALUE));
        val isrIds = (0 until numIsr).map(_ = > buffer.getInt);
        val isr = isrIds.map(brokers);

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
        val leaderId = if (leader.isDefined) leader.get.id
        else TopicMetadata.NoLeaderNodeId;
        buffer.putInt(leaderId);

    /* number of replicas */
        buffer.putInt(replicas.size);
        replicas.foreach(r = > buffer.putInt(r.id))

    /* number of in-sync replicas */
        buffer.putInt(isr.size);
        isr.foreach(r = > buffer.putInt(r.id))
    }

    @Override
    public String toString() {
        StringBuilder partitionMetadataString = new StringBuilder();
        partitionMetadataString.append("\tpartition " + partitionId);
        partitionMetadataString.append("\tleader: " + (leader.isPresent() ? formatBroker(leader.get()) : "none"));
        partitionMetadataString.append("\treplicas: " + Utils.map(replicas, this::formatBroker));
        partitionMetadataString.append("\tisr: " + Utils.map(isr, this::formatBroker));
        partitionMetadataString.append(String.format("\tisUnderReplicated: %s", isr.size() < replicas.size() ? "true" : "false"))
        return partitionMetadataString.toString();
    }

    private String formatBroker(Broker broker) {
        return broker.id + " (" + formatAddress(broker.host, broker.port) + ")";
    }
}
