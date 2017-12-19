package kafka.javaapi;

/**
 * @author zhoulf
 * @create 2017-12-19 20:16
 **/

class FetchResponse(private val underlying: kafka.api.FetchResponse) {

        def messageSet(topic: String, partition: Int): kafka.javaapi.message.ByteBufferMessageSet = {
        import Implicits._
        underlying.messageSet(topic, partition)
        }

        def highWatermark(topic: String, partition: Int) = underlying.highWatermark(topic, partition)

        def hasError = underlying.hasError

        def errorCode(topic: String, partition: Int) = underlying.errorCode(topic, partition)

        override def equals(other: Any) = canEqual(other) && {
        val otherFetchResponse = other.asInstanceOf[kafka.javaapi.FetchResponse]
        this.underlying.equals(otherFetchResponse.underlying)
        }

        def canEqual(other: Any) = other.isInstanceOf[kafka.javaapi.FetchResponse]

        override def hashCode = underlying.hashCode
        }
