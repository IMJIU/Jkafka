package kafka.cache;/**
 * Created by zhoulf on 2017/5/9.
 */

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author
 * @create 2017-05-09 16:41
 **/
public class FileCacheItem implements Comparable<FileCacheItem> {
    public static final ReentrantLock lock = new ReentrantLock();
    public volatile File file;
    public FileChannel channel;
    public Integer start;
    public Integer end;
    public Boolean isSlice;
    /* the size of the message set in bytes */
    private AtomicInteger _size = new AtomicInteger(0);

    public FileCacheItem(File file, FileChannel channel, Integer start, Integer end, Boolean isSlice) {
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
    public int compareTo(FileCacheItem o) {
        return start - o.start;
    }

    public int write(ByteBuffer buffer) {
        lock.lock();
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
            lock.unlock();
        }
        _size.getAndAdd(written);
        return written;
    }

    public ByteBuffer read() throws IOException {
        lock.lock();
        try{
            long pos = channel.position();
            channel.position(start);
            Integer newSize = end - start;
            ByteBuffer byteBuffer = ByteBuffer.allocate(newSize);
            channel.read(byteBuffer);
            channel.position(pos);
            return byteBuffer;
        }finally {
            lock.unlock();
        }
    }

    public int size() {
        return _size.get();
    }
}
