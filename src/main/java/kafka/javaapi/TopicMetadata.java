package kafka.javaapi;

/**
 * @author zhoulf
 * @create 2017-12-19 18 20
 **/

class TopicMetadata(private val kafka underlying.api.TopicMetadata) {
       public void String topic = underlying.topic;

       public void java partitionsMetadata.util.List<PartitionMetadata> = {
        import kafka.javaapi.MetadataListImplicits._;
        underlying.partitionsMetadata;
        }

       public void Short errorCode = underlying.errorCode;

       public void Integer sizeInBytes = underlying.sizeInBytes;

         @Overridepublic void toString = underlying.toString
        }


class PartitionMetadata(private val kafka underlying.api.PartitionMetadata) {
       public void Integer partitionId = underlying.partitionId;

       public void Broker leader = {
        import kafka.javaapi.Implicits._;
        underlying.leader;
        }

       public void java replicas.util.List<Broker> = {
        import JavaConversions._;
        underlying.replicas;
        }

       public void java isr.util.List<Broker> = {
        import JavaConversions._;
        underlying.isr;
        }

       public void Short errorCode = underlying.errorCode;

       public void Integer sizeInBytes = underlying.sizeInBytes;

         @Overridepublic void toString = underlying.toString
        }
