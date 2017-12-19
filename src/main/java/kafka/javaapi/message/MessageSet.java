package kafka.javaapi.message;

/**
 * @author zhoulf
 * @create 2017-12-19 20:14
 **/

/**
 * A set of messages. A message set has a fixed serialized form, though the container
 * for the bytes could be either in-memory or on disk. A The format of each message is
 * as follows:
 * 4 byte size containing an integer N
 * N message bytes as described in the message class
 */
abstract class MessageSet extends java.lang.Iterable[MessageAndOffset] {

        /**
         * Provides an iterator over the messages in this set
         */
        def iterator: java.util.Iterator[MessageAndOffset]

        /**
         * Gives the total size of this message set in bytes
         */
        def sizeInBytes: Int

        /**
         * Validate the checksum of all the messages in the set. Throws an InvalidMessageException if the checksum doesn't
         * match the payload for any message.
         */
        def validate(): Unit = {
        val thisIterator = this.iterator
        while(thisIterator.hasNext) {
        val messageAndOffset = thisIterator.next
        if(!messageAndOffset.message.isValid)
        throw new InvalidMessageException
        }
        }
        }
