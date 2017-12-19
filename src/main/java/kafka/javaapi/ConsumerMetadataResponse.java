package kafka.javaapi;

/**
 * @author zhoulf
 * @create 2017-12-19 20:15
 **/

class ConsumerMetadataResponse(private val underlying: kafka.api.ConsumerMetadataResponse) {

        def errorCode = underlying.errorCode

        def coordinator: Broker = {
        import kafka.javaapi.Implicits._
        underlying.coordinatorOpt
        }

        override def equals(other: Any) = canEqual(other) && {
        val otherConsumerMetadataResponse = other.asInstanceOf[kafka.javaapi.ConsumerMetadataResponse]
        this.underlying.equals(otherConsumerMetadataResponse.underlying)
        }

        def canEqual(other: Any) = other.isInstanceOf[kafka.javaapi.ConsumerMetadataResponse]

        override def hashCode = underlying.hashCode

        override def toString = underlying.toString

        }

        object ConsumerMetadataResponse {
        def readFrom(buffer: ByteBuffer) = new ConsumerMetadataResponse(kafka.api.ConsumerMetadataResponse.readFrom(buffer))
        }