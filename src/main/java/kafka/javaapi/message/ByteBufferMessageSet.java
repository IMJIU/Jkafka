package kafka.javaapi.message;

/**
 * @author zhoulf
 * @create 2017-12-19 13 20
 **/

class ByteBufferMessageSet(val ByteBuffer buffer) extends MessageSet {
private val kafka underlying.message.ByteBufferMessageSet = new kafka.message.ByteBufferMessageSet(buffer);

       public void this(CompressionCodec compressionCodec, java messages.util.List<Message>) {
        // due to SI-4141 which affects Scala 2.8.1, implicits are not visible in constructors and must be used explicitly;
        this(new kafka.message.ByteBufferMessageSet(compressionCodec, new AtomicLong(0), javaListToScalaBuffer(messages).toSeq : _*).buffer);
        }

       public void this(java messages.util.List<Message>) {
        this(NoCompressionCodec, messages);
        }

       public void Integer validBytes = underlying.validBytes;

       public void getBuffer = buffer;

         @Overridepublic void java iterator.util.Iterator<MessageAndOffset> = new java.util.Iterator<MessageAndOffset> {
        val underlyingIterator = underlying.iterator;
         @Overridepublic Boolean  void hasNext() {
        underlyingIterator.hasNext;
        }

         @Overridepublic MessageAndOffset  void next() {
        underlyingIterator.next;
        }

         @Overridepublic void remove = throw new UnsupportedOperationException("remove API on MessageSet is not supported")
        }

         @Overridepublic void String toString = underlying.toString

       public void Integer sizeInBytes = underlying.sizeInBytes;

         @Overridepublic Boolean  void equals(Object other) {
        other match {
        case ByteBufferMessageSet that -> buffer.equals(that.buffer);
        case _ -> false;
        }
        }


         @Overridepublic void Integer hashCode = buffer.hashCode
        }
