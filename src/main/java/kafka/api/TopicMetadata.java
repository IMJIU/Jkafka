package kafka.api;

import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.func.IntCount;
import kafka.func.Tuple;
import kafka.utils.Logging;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static kafka.api.ApiUtils.*;

/**
 * @author zhoulf
 * @create 2017-10-19 07 17
 **/

public class TopicMetadata extends Logging {
    public String topic;
    public List<PartitionMetadata> partitionsMetadata;
    public short errorCode = ErrorMapping.NoError;
    public static final Integer NoLeaderNodeId = -1;

    public TopicMetadata(String topic, List<PartitionMetadata> partitionsMetadata) {
        this(topic, partitionsMetadata, ErrorMapping.NoError);
    }

    public TopicMetadata(String topic, List<PartitionMetadata> partitionsMetadata, Short errorCode) {
        this.topic = topic;
        this.partitionsMetadata = partitionsMetadata;
        this.errorCode = errorCode;
    }

    public static TopicMetadata readFrom(ByteBuffer buffer, Map<Integer, Broker> brokers) {
        short errorCode = readShortInRange(buffer, "error code", Tuple.of((short) -1, Short.MAX_VALUE));
        String topic = readShortString(buffer);
        int numPartitions = readIntInRange(buffer, "number of partitions", Tuple.of(0, Integer.MAX_VALUE));
        PartitionMetadata[] partitionsMetadata = new PartitionMetadata[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
            PartitionMetadata partitionMetadata = PartitionMetadata.readFrom(buffer, brokers);
            //todo 顺序写入？？partitionsMetadata(partitionMetadata.partitionId) = partitionMetadata;
            partitionsMetadata[partitionMetadata.partitionId] = partitionMetadata;
        }
        return new TopicMetadata(topic, Arrays.asList(partitionsMetadata), errorCode);
    }

    public Integer sizeInBytes() {
        IntCount count = IntCount.of(0);
        partitionsMetadata.forEach(p -> count.add(p.sizeInBytes()));
        return 2 /* error code */ +
                shortStringLength(topic) +
                4 + count.get(); /* size and partition data array */
    }

    public void writeTo(ByteBuffer buffer) {
    /* error code */
        buffer.putShort(errorCode);
    /* topic */
        writeShortString(buffer, topic);
    /* number of partitions */
        buffer.putInt(partitionsMetadata.size());
        partitionsMetadata.forEach(m -> m.writeTo(buffer));
    }

    @Override
    public String toString() {
        StringBuilder topicMetadataInfo = new StringBuilder();
        topicMetadataInfo.append(String.format("{TopicMetadata for topic %s -> ", topic));
        if (errorCode == ErrorMapping.NoError) {
            partitionsMetadata.forEach(partitionMetadata -> {
                if (ErrorMapping.NoError == partitionMetadata.errorCode) {
                    topicMetadataInfo.append(String.format("\nMetadata for partition <%s,%d> is %s", topic,
                            partitionMetadata.partitionId, partitionMetadata.toString()));
                } else if (ErrorMapping.ReplicaNotAvailableCode == partitionMetadata.errorCode) {
                    // this error message means some replica other than the leader is not available. The consumer;
                    // doesn't care about non leader replicas, so ignore this;
                    topicMetadataInfo.append(String.format("\nMetadata for partition <%s,%d> is %s", topic,
                            partitionMetadata.partitionId, partitionMetadata.toString()));
                } else {
                    topicMetadataInfo.append(String.format("\nMetadata for partition <%s,%d> is not available due to %s", topic,
                            partitionMetadata.partitionId, ErrorMapping.exceptionFor(partitionMetadata.errorCode).getClass().getName()));
                }
            });
        } else {
            topicMetadataInfo.append(String.format("\nNo partition metadata for topic %s due to %s", topic,
                    ErrorMapping.exceptionFor(errorCode).getClass().getName()));
        }
        topicMetadataInfo.append("}");
        return topicMetadataInfo.toString();
    }
}
