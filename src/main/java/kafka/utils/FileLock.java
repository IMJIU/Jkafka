package kafka.utils;/**
 * Created by zhoulf on 2017/4/17.
 */

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;

/**
 * A file lock a la flock/funlock
 * <p>
 * The given path will be created and opened if it doesn't exist.
 */
public class FileLock extends Logging {
    public File file;

    public FileLock(File file) throws IOException {
        this.file = file;
        init();
    }

    public void init() throws IOException {
        file.createNewFile(); // create the file if it doesn't exist;
        channel = new RandomAccessFile(file, "rw").getChannel();
    }


    private FileChannel channel;
    private java.nio.channels.FileLock flock = null;

    /**
     * Lock the file or throw an exception if the lock is already held
     */
    public void lock() throws IOException {
        synchronized (this) {
            trace("Acquiring lock on " + file.getAbsolutePath());
            flock = channel.lock();
        }
    }

    /**
     * Try to lock the file and return true if the locking succeeds
     */
    public Boolean tryLock() {
        synchronized (this) {
            trace("Acquiring lock on " + file.getAbsolutePath());
            try {
                // weirdly this method will return null if the lock is held by another;
                // process, but will throw an exception if the lock is held by this process;
                // so we have to handle both cases;
                flock = channel.tryLock();
                return flock != null;
            } catch (IOException e) {
                e.printStackTrace();
            } catch (OverlappingFileLockException e) {
                return false;
            }
        }
        return false;
    }

    /**
     * Unlock the lock if it is held
     */
    public void unlock() throws IOException {
        synchronized (this) {
            trace("Releasing lock on " + file.getAbsolutePath());
            if (flock != null)
                flock.release();
        }
    }

    /**
     * Destroy this lock, closing the associated FileChannel
     */
    public void destroy() throws IOException {
        synchronized (this) {
            unlock();
            channel.close();
        }
    }
}
