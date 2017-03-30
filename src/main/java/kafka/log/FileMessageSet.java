package kafka.log;

import kafka.common.KafkaException;
import kafka.message.*;
import kafka.utils.IteratorTemplate;
import kafka.utils.Utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Administrator on 2017/3/26.
 */
public class FileMessageSet extends MessageSet {
    public volatile File file;
    public FileChannel channel;
    public Integer start;
    public Integer end;
    public Boolean isSlice;

    public FileMessageSet(File file, FileChannel channel, Integer start, Integer end, Boolean isSlice) {
        this.file = file;
        this.channel = channel;
        this.start = start;
        this.end = end;
        this.isSlice = isSlice;
        init();
    }

    private void init() {
        try {
            if (isSlice) {
                _size = new AtomicInteger(end - start); // don't check the file size if this is just a slice view
            } else {
                _size = new AtomicInteger((int) Math.min(channel.size(), end) - start);
            }
            /* if this is not a slice, update the file pointer to the end of the file */
            if (!isSlice) {
                /* set the file position to the last byte in the file */
                channel.position(channel.size());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Create a file message set with no slicing.
     */
    public FileMessageSet(File file, FileChannel channel) {
        this(file, channel, 0, Integer.MAX_VALUE, false);
    }


    /* the size of the message set in bytes */
    private AtomicInteger _size;


    /**
     * Create a file message set with no slicing
     */
    public FileMessageSet(File file) throws FileNotFoundException {
        this(file, Utils.openChannel(file, true));
    }

    /**
     * Create a file message set with mutable option
     */
    public FileMessageSet(File file, boolean mutable) throws FileNotFoundException {
        this(file, Utils.openChannel(file, mutable));
    }

    /**
     * Create a slice view of the file message set that begins and ends at the given byte offsets
     */
    public FileMessageSet(File file, FileChannel channel, Integer start, Integer end) {
        this(file, channel, start, end, true);
    }


    /**
     * Return a message set which is a view into this set starting from the given position and with the given size limit.
     * <p>
     * If the size is beyond the end of the file, the end will be based on the size of the file at the time of the read.
     * <p>
     * If this message set is already sliced, the position will be taken relative to that slicing.
     *
     * @param position The start position to begin the read from
     * @param size     The number of bytes after the start position to include
     * @return A sliced wrapper on this message set limited based on the given position and size
     */
    public FileMessageSet read(Integer position, Integer size) {
        if (position < 0) {
            throw new IllegalArgumentException("Invalid position: " + position);
        }
        if (size < 0) {
            throw new IllegalArgumentException("Invalid size: " + size);
        }
        return new FileMessageSet(file, channel,
                this.start + position,
                Math.min(this.start + position + size, sizeInBytes()));
    }

    /**
     * Search forward for the file position of the last offset that is greater than or equal to the target offset
     * and return its physical position. If no such offsets are found, return null.
     *
     * @param targetOffset     The offset to search for.
     * @param startingPosition The starting position in the file to begin searching from.
     */
    public OffsetPosition searchFor(Long targetOffset, Integer startingPosition) {
        Integer position = startingPosition;
        ByteBuffer buffer = ByteBuffer.allocate(MessageSet.LogOverhead);
        Integer size = sizeInBytes();
        try {
            while (position + MessageSet.LogOverhead < size) {
                buffer.rewind();
                channel.read(buffer, position);
                if (buffer.hasRemaining())
                    throw new IllegalStateException("Failed to read complete buffer for targetOffset %d startPosition %d in %s"
                            .format(targetOffset.toString(), startingPosition, file.getAbsolutePath()));
                buffer.rewind();
                Long offset = buffer.getLong();
                if (offset >= targetOffset)
                    return new OffsetPosition(offset, position);
                Integer messageSize = buffer.getInt();
                if (messageSize < Message.MessageOverhead)
                    throw new IllegalStateException("Invalid message size: " + messageSize);
                position += MessageSet.LogOverhead + messageSize;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Write some of this set to the given channel.
     *
     * @param destChannel   The channel to write to.
     * @param writePosition The position in the message set to begin writing from.
     * @param size          The maximum number of bytes to write
     * @return The number of bytes actually written.
     */
    public Integer writeTo(GatheringByteChannel destChannel, Long writePosition, Integer size) throws IOException {
        // Ensure that the underlying size has not changed.
        Integer newSize = Math.min((int) channel.size(), end) - start;
        if (newSize < _size.get()) {
            throw new KafkaException("Size of FileMessageSet %s has been truncated during write: old size %d, new size %d"
                    .format(file.getAbsolutePath(), _size.get(), newSize));
        }
        Integer bytesTransferred = (int) channel.transferTo(start + writePosition, Math.min(size, sizeInBytes()), destChannel);
        trace("FileMessageSet " + file.getAbsolutePath() + " : bytes transferred : " + bytesTransferred
                + " bytes requested for transfer : " + Math.min(size, sizeInBytes()));
        return bytesTransferred;
    }

    /**
     * Get a shallow iterator over the messages in the set.
     */
    public Iterator<MessageAndOffset> iterator() {
        return iterator(Integer.MAX_VALUE);
    }


    /**
     * Get an iterator over the messages in the set. We only do shallow iteration here.
     *
     * @param maxMessageSize A limit on allowable message size to avoid allocating unbounded memory.
     *                       If we encounter a message larger than this we throw an InvalidMessageException.
     * @return The iterator.
     */
    @SuppressWarnings("unchecked")
    public Iterator<MessageAndOffset> iterator(final Integer maxMessageSize) {
        return new IteratorTemplate<MessageAndOffset>() {
            Integer location = start;
            ByteBuffer sizeOffsetBuffer = ByteBuffer.allocate(12);

            @Override
            protected MessageAndOffset makeNext() {
                if (location >= end)
                    return allDone();
                try {
                    // read the size of the item
                    sizeOffsetBuffer.rewind();
                    channel.read(sizeOffsetBuffer, location);
                    if (sizeOffsetBuffer.hasRemaining())
                        return allDone();

                    sizeOffsetBuffer.rewind();
                    Long offset = sizeOffsetBuffer.getLong();
                    Integer size = sizeOffsetBuffer.getInt();
                    if (size < Message.MinHeaderSize)
                        return allDone();
                    if (size > maxMessageSize)
                        throw new InvalidMessageException("Message size exceeds the largest allowable message size (%d).".format(maxMessageSize.toString()));

                    // read the item itself
                    ByteBuffer buffer = ByteBuffer.allocate(size);
                    channel.read(buffer, location + 12);
                    if (buffer.hasRemaining())
                        return allDone();
                    buffer.rewind();

                    // increment the location and return the item
                    location += size + 12;
                    return new MessageAndOffset(new Message(buffer), offset);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return null;
            }
        };
    }

    /**
     * The number of bytes taken up by this file set
     */
    public Integer sizeInBytes() {
        return _size.get();
    }

    /**
     * Append these messages to the message set
     */
    public void append(ByteBufferMessageSet messages) {
        try {
            int written = messages.writeTo(channel, 0L, messages.sizeInBytes());
            _size.getAndAdd(written);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Commit all written data to the physical disk
     */
    public void flush() throws IOException {
        channel.force(true);
    }

    /**
     * Close this message set
     */
    public void close() throws IOException {
        flush();
        channel.close();
    }

    /**
     * Delete this message set from the filesystem
     *
     * @return True iff this message set was deleted.
     */
    public Boolean delete() {
        try {
            channel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return file.delete();
    }

    /**
     * Truncate this file message set to the given size in bytes. Note that this API does no checking that the
     * given size falls on a valid message boundary.
     *
     * @param targetSize The size to truncate to.
     * @return The number of bytes truncated off
     */
    public Integer truncateTo(Integer targetSize) throws IOException {
        Integer originalSize = sizeInBytes();
        if (targetSize > originalSize || targetSize < 0)
            throw new KafkaException("Attempt to truncate log segment to " + targetSize + " bytes failed, " +
                    " size of this log segment is " + originalSize + " bytes.");
        channel.truncate(targetSize);
        channel.position(targetSize);
        _size.set(targetSize);
        return originalSize - targetSize;
    }

    /**
     * Read from the underlying file into the buffer starting at the given position
     */
    public ByteBuffer readInto(ByteBuffer buffer, Integer relativePosition) throws IOException {
        channel.read(buffer, relativePosition + this.start);
        buffer.flip();
        return buffer;
    }

    /**
     * Rename the file that backs this message set
     *
     * @return true iff the rename was successful
     */
    public boolean renameTo(File f) {
        boolean success = this.file.renameTo(f);
        this.file = f;
        return success;
    }
}
