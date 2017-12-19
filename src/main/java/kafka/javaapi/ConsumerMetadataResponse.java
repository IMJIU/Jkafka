package kafka.javaapi;

/**
 * @author zhoulf
 * @create 2017-12-19 15 20
 **/

class ConsumerMetadataResponse(private val kafka underlying.api.ConsumerMetadataResponse) {

       public void errorCode = underlying.errorCode;

       public void Broker coordinator = {
        import kafka.javaapi.Implicits._;
        underlying.coordinatorOpt;
        }

         @Overridepublic void equals(Object other) = canEqual(other) && {
        val otherConsumerMetadataResponse = other.asInstanceOf<kafka.javaapi.ConsumerMetadataResponse>
        this.underlying.equals(otherConsumerMetadataResponse.underlying);
        }

       public void canEqual(Object other) = other.isInstanceOf<kafka.javaapi.ConsumerMetadataResponse>

         @Overridepublic void hashCode = underlying.hashCode

         @Overridepublic void toString = underlying.toString

        }

        object ConsumerMetadataResponse {
       public void readFrom(ByteBuffer buffer) = new ConsumerMetadataResponse(kafka.api.ConsumerMetadataResponse.readFrom(buffer));
        }
