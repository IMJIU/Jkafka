package kafka.network;

/**
 * Created by Administrator on 2017/4/20.
 */
public class Transmission extends Logging {

        def complete: Boolean

        protected def expectIncomplete(): Unit = {
        if(complete)
            throw new KafkaException("This operation cannot be completed on a complete request.")
    }

    protected def expectComplete(): Unit = {
        if(!complete)
            throw new KafkaException("This operation cannot be completed on an incomplete request.")
    }

}
}
