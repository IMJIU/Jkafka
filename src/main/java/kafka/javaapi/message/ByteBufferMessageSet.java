package kafka.javaapi.message;

/**
 * @author zhoulf
 * @create 2017-12-19 20:13
 **/

class ByteBufferMessageSet(val buffer: ByteBuffer) extends MessageSet {
private val underlying: kafka.message.ByteBufferMessageSet = new kafka.message.ByteBufferMessageSet(buffer)

        def this(compressionCodec: CompressionCodec, messages: java.util.List[Message]) {
        // due to SI-4141 which affects Scala 2.8.1, implicits are not visible in constructors and must be used explicitly
        this(new kafka.message.ByteBufferMessageSet(compressionCodec, new AtomicLong(0), javaListToScalaBuffer(messages).toSeq : _*).buffer)
        }

        def this(messages: java.util.List[Message]) {
        this(NoCompressionCodec, messages)
        }

        def validBytes: Int = underlying.validBytes

        def getBuffer = buffer

        override def iterator: java.util.Iterator[MessageAndOffset] = new java.util.Iterator[MessageAndOffset] {
        val underlyingIterator = underlying.iterator
        override def hasNext(): Boolean = {
        underlyingIterator.hasNext
        }

        override def next(): MessageAndOffset = {
        underlyingIterator.next
        }

        override def remove = throw new UnsupportedOperationException("remove API on MessageSet is not supported")
        }

        override def toString: String = underlying.toString

        def sizeInBytes: Int = underlying.sizeInBytes

        override def equals(other: Any): Boolean = {
        other match {
        case that: ByteBufferMessageSet => buffer.equals(that.buffer)
        case _ => false
        }
        }


        override def hashCode: Int = buffer.hashCode
        }