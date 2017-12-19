package kafka.javaapi;

/**
 * @author zhoulf
 * @create 2017-12-19 16 20
 **/

class FetchResponse(private val kafka underlying.api.FetchResponse) {

       public void messageSet(String topic, Int partition): kafka.javaapi.message.ByteBufferMessageSet = {
        import Implicits._;
        underlying.messageSet(topic, partition);
        }

       public void highWatermark(String topic, Int partition) = underlying.highWatermark(topic, partition);

       public void hasError = underlying.hasError;

       public void errorCode(String topic, Int partition) = underlying.errorCode(topic, partition);

         @Overridepublic void equals(Object other) = canEqual(other) && {
        val otherFetchResponse = other.asInstanceOf<kafka.javaapi.FetchResponse>
        this.underlying.equals(otherFetchResponse.underlying);
        }

       public void canEqual(Object other) = other.isInstanceOf<kafka.javaapi.FetchResponse>

         @Overridepublic void hashCode = underlying.hashCode
        }
