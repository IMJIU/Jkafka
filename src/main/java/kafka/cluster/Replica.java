package kafka.cluster;

import kafka.common.KafkaException;
import kafka.log.Log;
import kafka.server.LogOffsetMetadata;
import kafka.utils.Logging;
import kafka.utils.Time;
import kafka.utils.Utils;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author
 * @create 2017-04-01 16 18
 **/
public class Replica extends Logging {
    public Integer brokerId;
    public Partition partition;
    public Time time;
    public Long initialHighWatermarkValue = 0L;
    public Optional<Log> log = Optional.empty();
    ///////////////////////////////////////////////////////////////
    public String topic;
    public Integer partitionId;

    public Replica(Integer brokerId, Partition partition, Time time) {
        this(brokerId, partition, time, 0L, Optional.empty());
    }

    public Replica(Integer brokerId, Partition partition, Time time, Long initialHighWatermarkValue, Optional<Log> log) {
        this.brokerId = brokerId;
        this.partition = partition;
        this.time = time;
        this.initialHighWatermarkValue = initialHighWatermarkValue;
        this.log = log;
        init();
    }

    public void init() {
        topic = partition.topic;
        partitionId = partition.partitionId;
    }


    //        // the high watermark offset value, in non-leader replicas only its message offsets are kept;
    private volatile LogOffsetMetadata highWatermarkMetadata = new LogOffsetMetadata(initialHighWatermarkValue);

    //        // the log end offset value, kept in all replicas;
//        // for local replica it is the log's end offset, for remote replicas its value is only updated by follower fetch;
    private volatile LogOffsetMetadata logEndOffsetMetadata = LogOffsetMetadata.UnknownOffsetMetadata;
    //        // the time when log offset is updated;
    private AtomicLong logEndOffsetUpdateTimeMsValue = new AtomicLong(time.milliseconds());


    //
    public Boolean isLocal() {
        if (log.isPresent()) {
            return true;
        }
        return false;
    }

    //
    public void logEndOffset_(LogOffsetMetadata newLogEndOffset) {
        if (isLocal()) {
            throw new KafkaException(String.format("Should not set log end offset on partition <%s,%d>'s local replica %d", topic, partitionId, brokerId));
        } else {
            logEndOffsetMetadata = newLogEndOffset;
            logEndOffsetUpdateTimeMsValue.set(time.milliseconds());
            logger.trace(String.format("Setting log end offset for replica %d for partition <%s,%d> to <%s>",
                    brokerId, topic, partitionId, logEndOffsetMetadata));
        }
    }

    //
    public LogOffsetMetadata logEndOffset() {
        if (isLocal())
            return log.get().logEndOffsetMetadata();
        else
            return logEndOffsetMetadata;
    }

    public long logEndOffsetUpdateTimeMs() {
        return logEndOffsetUpdateTimeMsValue.get();
    }


    public void highWatermark_(LogOffsetMetadata newHighWatermark) {
        if (isLocal()) {
            highWatermarkMetadata = newHighWatermark;
            trace(String.format("Setting high watermark for replica %d partition <%s,%d> on broker %d to <%s>",
                    brokerId, topic, partitionId, brokerId, newHighWatermark));
        } else {
            throw new KafkaException(String.format("Should not set high watermark on partition <%s,%d>'s non-local replica %d", topic, partitionId, brokerId));
        }
    }

    public LogOffsetMetadata highWatermark = highWatermarkMetadata;


    public void convertHWToLocalOffsetMetadata() {
        if (isLocal()) {
            highWatermarkMetadata = log.get().convertToOffsetMetadata(highWatermarkMetadata.messageOffset);
        } else {
            throw new KafkaException(String.format("Should not construct complete high watermark on partition <%s,%d>'s non-local replica %d", topic, partitionId, brokerId));
        }
    }


    @Override
    public boolean equals(Object that) {
        if (!(that instanceof Replica))
            return false;
        Replica other = (Replica) that;
        if (topic.equals(other.topic) && brokerId == other.brokerId && partition.equals(other.partition))
            return true;
        return false;
    }

    @Override
    public int hashCode() {
        return 31 + topic.hashCode() + 17 * brokerId + partition.hashCode();
    }

    @Override
    public String toString() {
        StringBuilder replicaString = new StringBuilder();
        replicaString.append("ReplicaId: " + brokerId);
        replicaString.append("; Topic: " + topic);
        replicaString.append("; Partition: " + partition.partitionId);
        replicaString.append("; isLocal: " + isLocal());
        if (isLocal()) replicaString.append("; Highwatermark: " + highWatermark);
        return replicaString.toString();
    }
}
