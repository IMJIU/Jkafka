package kafka.cache.file;
/**
 * Created by zhoulf on 2017/5/9.
 */
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class FileBuffer implements Comparable<FileBuffer> {
    public static Logger logger = LoggerFactory.getLogger(FileBuffer.class.getName());
    public static final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    public volatile File file;
    public FileChannel channel;
    public Integer start;
    public Integer end;
    public Boolean isSlice;
    /* the size of the message set in bytes */
    private AtomicInteger _size = new AtomicInteger(0);

    public FileBuffer(File file, FileChannel channel, Integer start, Integer end, Boolean isSlice) {
        this.file = file;
        this.channel = channel;
        this.start = start;
        this.end = end;
        this.isSlice = isSlice;
        try {
            if (isSlice) {
                _size = new AtomicInteger(end - start); // don't check the file size if this is just a slice view
            } else {
                _size = new AtomicInteger((int) channel.size());
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

    @Override
    public int compareTo(FileBuffer o) {
        return start - o.start;
    }

    public int write(ByteBuffer buffer) {
        lock.writeLock().lock();
        // Ignore offset and size from input. We just want to write the whole buffer to the channel.
        buffer.mark();
        int written = 0;
        try {
            while (buffer.hasRemaining()) {
                written += channel.write(buffer);
            }
            buffer.reset();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            lock.writeLock().unlock();
        }
        _size.getAndAdd(written);
        return written;
    }

    public ByteBuffer read() throws IOException {
        lock.readLock().lock();
        try {
            long pos = channel.position();
            channel.position(start);
            Integer newSize = end - start;
            ByteBuffer byteBuffer = ByteBuffer.allocate(newSize);
            channel.read(byteBuffer);
            channel.position(pos);
            return byteBuffer;
        } finally {
            lock.readLock().unlock();
        }
    }

    public int size() {
        return _size.get();
    }


    public long transferTo(GatheringByteChannel destChannel) throws IOException {
        return writeTo(destChannel, 0L, sizeInBytes());
    }

    public long writeTo(GatheringByteChannel destChannel, Long writePosition, Integer size) throws IOException {
        // Ensure that the underlying size has not changed.
        Integer newSize = Math.min((int) channel.size(), end) - start;
        if (newSize < _size.get()) {
            throw new IOException("Size of FileMessageSet %s has been truncated during write: old size %d, new size %d"
                    .format(file.getAbsolutePath(), _size.get(), newSize));
        }
        long bytesTransferred = channel.transferTo(start + writePosition, Math.min(size, sizeInBytes()), destChannel);
        logger.trace("FileMessageSet " + file.getAbsolutePath() + " : bytes transferred : " + bytesTransferred
                + " bytes requested for transfer : " + Math.min(size, sizeInBytes()));
        return bytesTransferred;
    }

    /**
     * The number of bytes taken up by this file set
     */
    public Integer sizeInBytes() {
        return _size.get();
    }
}
