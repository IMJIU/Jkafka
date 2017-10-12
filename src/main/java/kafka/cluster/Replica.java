package kafka.cluster;

import kafka.log.Log;
import kafka.server.LogOffsetMetadata;
import kafka.utils.Time;

import java.util.Optional;

/**
 * @author
 * @create 2017-04-01 16 18
 **/
public class Replica {
    public Integer brokerId;
    public Partition partition;
    public Time time;
    public Long initialHighWatermarkValue = 0L;
    public Optional<Log> log = Optional.empty();
    ///////////////////////////////////////////////////////////////
    public String topic ;
    public Integer partitionId ;

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
//  @volatile private<this> var LogOffsetMetadata logEndOffsetMetadata = LogOffsetMetadata.UnknownOffsetMetadata
//        // the time when log offset is updated;
//        private<this> val logEndOffsetUpdateTimeMsValue = new AtomicLong(time.milliseconds);
//
//        val topic = partition.topic;
//        val partitionId = partition.partitionId;
//
//        public void  Boolean isLocal = {
//                log match {
//        case Some(l) => true;
//        case None => false;
//    }
//  }
//
//            public void  logEndOffset_=(LogOffsetMetadata newLogEndOffset) {
//            if (isLocal) {
//                throw new KafkaException(String.format("Should not set log end offset on partition <%s,%d>'s local replica %d",topic, partitionId, brokerId))
//            } else {
//                logEndOffsetMetadata = newLogEndOffset;
//                logEndOffsetUpdateTimeMsValue.set(time.milliseconds);
//                trace("Setting log end offset for replica %d for partition <%s,%d> to <%s>";
//                        .format(brokerId, topic, partitionId, logEndOffsetMetadata))
//            }
//        }
//
//        public void  logEndOffset =
//        if (isLocal)
//            log.get.logEndOffsetMetadata;
//        else;
//            logEndOffsetMetadata;
//
//        public void  logEndOffsetUpdateTimeMs = logEndOffsetUpdateTimeMsValue.get();
//
//        public void  highWatermark_=(LogOffsetMetadata newHighWatermark) {
//            if (isLocal) {
//                highWatermarkMetadata = newHighWatermark;
//                trace("Setting high watermark for replica %d partition <%s,%d> on broker %d to <%s>";
//                        .format(brokerId, topic, partitionId, brokerId, newHighWatermark))
//            } else {
//                throw new KafkaException(String.format("Should not set high watermark on partition <%s,%d>'s non-local replica %d",topic, partitionId, brokerId))
//            }
//        }
//
        public LogOffsetMetadata  highWatermark = highWatermarkMetadata;
//
//        public void  convertHWToLocalOffsetMetadata() = {
//        if (isLocal) {
//            highWatermarkMetadata = log.get.convertToOffsetMetadata(highWatermarkMetadata.messageOffset);
//        } else {
//            throw new KafkaException(String.format("Should not construct complete high watermark on partition <%s,%d>'s non-local replica %d",topic, partitionId, brokerId))
//        }
//  }
//
//        override public void  equals(Any that): Boolean = {
//        if(!(that.isInstanceOf<Replica>))
//            return false;
//        val other = that.asInstanceOf<Replica>;
//        if(topic.equals(other.topic) && brokerId == other.brokerId && partition.equals(other.partition))
//            return true;
//        false;
//  }
//
//        override public void  hashCode(): Integer = {
//                31 + topic.hashCode() + 17*brokerId + partition.hashCode();
//        }
//
//
//        override public void  toString(): String = {
//                val replicaString = new StringBuilder;
//                replicaString.append("ReplicaId: " + brokerId);
//                replicaString.append("; Topic: " + topic);
//                replicaString.append("; Partition: " + partition.partitionId);
//                replicaString.append("; isLocal: " + isLocal);
//        if(isLocal) replicaString.append("; Highwatermark: " + highWatermark)
//        replicaString.toString();
//  }
}
