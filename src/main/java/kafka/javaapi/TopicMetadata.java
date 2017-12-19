package kafka.javaapi;

/**
 * @author zhoulf
 * @create 2017-12-19 20:18
 **/

class TopicMetadata(private val underlying: kafka.api.TopicMetadata) {
        def topic: String = underlying.topic

        def partitionsMetadata: java.util.List[PartitionMetadata] = {
        import kafka.javaapi.MetadataListImplicits._
        underlying.partitionsMetadata
        }

        def errorCode: Short = underlying.errorCode

        def sizeInBytes: Int = underlying.sizeInBytes

        override def toString = underlying.toString
        }


class PartitionMetadata(private val underlying: kafka.api.PartitionMetadata) {
        def partitionId: Int = underlying.partitionId

        def leader: Broker = {
        import kafka.javaapi.Implicits._
        underlying.leader
        }

        def replicas: java.util.List[Broker] = {
        import JavaConversions._
        underlying.replicas
        }

        def isr: java.util.List[Broker] = {
        import JavaConversions._
        underlying.isr
        }

        def errorCode: Short = underlying.errorCode

        def sizeInBytes: Int = underlying.sizeInBytes

        override def toString = underlying.toString
        }
