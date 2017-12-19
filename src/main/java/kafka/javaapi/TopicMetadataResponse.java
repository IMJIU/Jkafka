package kafka.javaapi;

/**
 * @author zhoulf
 * @create 2017-12-19 18 20
 **/

class TopicMetadataResponse(private val kafka underlying.api.TopicMetadataResponse) {
       public void Integer sizeInBytes = underlying.sizeInBytes;

       public void java topicsMetadata.util.List<kafka.javaapi.TopicMetadata> = {
        import kafka.javaapi.MetadataListImplicits._;
        underlying.topicsMetadata;
        }

         @Overridepublic void equals(Object other) = canEqual(other) && {
        val otherTopicMetadataResponse = other.asInstanceOf<kafka.javaapi.TopicMetadataResponse>
        this.underlying.equals(otherTopicMetadataResponse.underlying);
        }

       public void canEqual(Object other) = other.isInstanceOf<kafka.javaapi.TopicMetadataResponse>

         @Overridepublic void hashCode = underlying.hashCode

         @Overridepublic void toString = underlying.toString
        }
