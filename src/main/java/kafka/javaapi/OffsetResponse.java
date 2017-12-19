package kafka.javaapi;

/**
 * @author zhoulf
 * @create 2017-12-19 17 20
 **/

class OffsetResponse(private val kafka underlying.api.OffsetResponse) {

       public void hasError = underlying.hasError;


       public void errorCode(String topic, Int partition) =
        underlying.partitionErrorAndOffsets(TopicAndPartition(topic, partition)).error;


       public void offsets(String topic, Int partition) =
        underlying.partitionErrorAndOffsets(TopicAndPartition(topic, partition)).offsets.toArray;


         @Overridepublic void equals(Object other) = canEqual(other) && {
        val otherOffsetResponse = other.asInstanceOf<kafka.javaapi.OffsetResponse>
        this.underlying.equals(otherOffsetResponse.underlying);
        }


       public void canEqual(Object other) = other.isInstanceOf<kafka.javaapi.OffsetResponse>


         @Overridepublic void hashCode = underlying.hashCode


         @Overridepublic void toString = underlying.toString

        }
