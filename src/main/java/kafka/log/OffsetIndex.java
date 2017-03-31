package kafka.log;

import kafka.common.InvalidOffsetException;
import kafka.func.Processor;
import kafka.utils.Logging;
import kafka.utils.Os;
import kafka.utils.Prediction;
import kafka.utils.Utils;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * An index that maps offsets to physical file locations for a particular log segment. This index may be sparse:
 * that is it may not hold an entry for all messages in the log.
 * <p>
 * The index is stored in a file that is pre-allocated to hold a fixed maximum number of 8-byte entries.
 * <p>
 * The index supports lookups against a memory-map of this file. These lookups are done using a simple binary search variant
 * to locate the offset/location pair for the greatest offset less than or equal to the target offset.
 * <p>
 * Index files can be opened in two ways: either as an empty, mutable index that allows appends or
 * an immutable read-only index file that has previously been populated. The makeReadOnly method will turn a mutable file into an
 * immutable one and truncate off any extra bytes. This is done when the index file is rolled over.
 * <p>
 * No attempt is made to checksum the contents of this file, in the event of a crash it is rebuilt.
 * <p>
 * The file format is a series of entries. The physical format is a 4 byte "relative" offset and a 4 byte file location for the
 * message with that offset. The offset stored is relative to the base offset of the index file. So, for example,
 * if the base offset was 50, then the offset 55 would be stored as 5. Using relative offsets in this way let's us use
 * only 4 bytes for the offset.
 * <p>
 * The frequency of entries is up to the user of this class.
 * <p>
 * All external APIs translate from relative offsets to full offsets, so users of this class do not interact with the internal
 * storage format.
 */
public class OffsetIndex extends Logging {
    public volatile File file;
    public Long baseOffset;
    public Integer maxIndexSize = -1;

    public OffsetIndex(File file, Long baseOffset) throws IOException {
        this(file, baseOffset, -1);
    }

    public OffsetIndex(File file, Long baseOffset, Integer maxIndexSize) throws IOException {
        this.file = file;
        this.baseOffset = baseOffset;
        this.maxIndexSize = maxIndexSize;
        init();
    }

    private ReentrantLock lock = new ReentrantLock();
    private MappedByteBuffer mmap;
    /* the number of eight-byte entries currently in the index */
    private AtomicInteger size;
    /**
     * The maximum number of eight-byte entries this index can hold
     */
    public volatile Integer maxEntries;

    /* the last offset in the index */
    public Long lastOffset;


    public void init() throws IOException {
        /* initialize the memory mapping for this index */
        boolean newlyCreated = file.createNewFile();
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        try {
        /* pre-allocate the file if necessary */
            if (newlyCreated) {
                if (maxIndexSize < 8)
                    throw new IllegalArgumentException("Invalid max index size: " + maxIndexSize);
                raf.setLength(roundToExactMultiple(maxIndexSize, 8));
            }
         /* memory-map the file */
            long len = raf.length();
            MappedByteBuffer idx = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, len);

        /* set the position in the index for the next entry */
            if (newlyCreated) {
                idx.position(0);
            } else {
                // if this is a pre-existing index, assume it is all valid and set position to last entry
                idx.position(roundToExactMultiple(idx.limit(), 8));
            }
            mmap = idx;
        } finally {
            raf.close();
        }
        size = new AtomicInteger(mmap.position() / 8);
        maxEntries = mmap.limit() / 8;
        lastOffset = readLastEntry().offset;
        debug("Loaded index file %s with maxEntries = %d, maxIndexSize = %d, entries = %d, lastOffset = %d, file position = %d"
                .format(file.getAbsolutePath(), maxEntries, maxIndexSize, entries(), lastOffset, mmap.position()));
    }

    /**
     * The last entry in the index
     */
    public OffsetPosition readLastEntry() {
        return Utils.inLock(lock, () -> {
            int s = size.get();
            if (s == 0) {
                return new OffsetPosition(baseOffset, 0);
            }
            return new OffsetPosition(baseOffset + relativeOffset(this.mmap, s - 1), physical(this.mmap, s - 1));
        });
    }

    /**
     * Find the largest offset less than or equal to the given targetOffset
     * and return a pair holding this offset and it's corresponding physical file position.
     *
     * @param targetOffset The offset to look up.
     * @return The offset found and the corresponding file position for this offset.
     * If the target offset is smaller than the least entry in the index (or the index is empty),
     * the pair (baseOffset, 0) is returned.
     */
    public OffsetPosition lookup(Long targetOffset) {
        return maybeLock(lock, () -> {
            ByteBuffer idx = mmap.duplicate();
            int slot = indexSlotFor(idx, targetOffset);
            if (slot == -1) {
                return new OffsetPosition(baseOffset, 0);
            } else {
                return new OffsetPosition(baseOffset + relativeOffset(idx, slot), physical(idx, slot));
            }
        });
    }

    /**
     * Find the slot in which the largest offset less than or equal to the given
     * target offset is stored.
     *
     * @param idx          The index buffer
     * @param targetOffset The offset to look for
     * @return The slot found or -1 if the least entry in the index is larger than the target offset or the index is empty
     */
    private Integer indexSlotFor(ByteBuffer idx, Long targetOffset) {
        // we only store the difference from the base offset so calculate that
        long relOffset = targetOffset - baseOffset;

        // check if the index is empty
        if (entries() == 0)
            return -1;

        // check if the target offset is smaller than the least offset
        if (relativeOffset(idx, 0) > relOffset)
            return -1;

        // binary search for the entry
        int lo = 0;
        int hi = entries() - 1;
        while (lo < hi) {
            int mid = (int) Math.ceil(hi / 2.0 + lo / 2.0);
            int found = relativeOffset(idx, mid);
            if (found == relOffset)
                return mid;
            else if (found < relOffset)
                lo = mid;
            else
                hi = mid - 1;
        }
        return lo;
    }

    /* return the nth offset relative to the base offset */
    private Integer relativeOffset(ByteBuffer buffer, Integer n) {
        return buffer.getInt(n * 8);
    }

    /* return the nth physical position */
    private Integer physical(ByteBuffer buffer, Integer n) {
        return buffer.getInt(n * 8 + 4);
    }

    /**
     * Get the nth offset mapping from the index
     *
     * @param n The entry number in the index
     * @return The offset/position pair at that entry
     */
    public OffsetPosition entry(Integer n) {
        return maybeLock(lock, () -> {
            if (n >= entries())
                throw new IllegalArgumentException("Attempt to fetch the %dth entry from an index of size %d.".format(n.toString(), entries()));
            ByteBuffer idx = mmap.duplicate();
            return new OffsetPosition(relativeOffset(idx, n).longValue(), physical(idx, n));
        });
    }

    /**
     * Append an entry for the given offset/location pair to the index. This entry must have a larger offset than all subsequent entries.
     */
    public void append(Long offset, Integer position) {
        Utils.inLock(lock, () -> {
            Prediction.require(!isFull(), "Attempt to append to a full index (size = " + size + ").");
            if (size.get() == 0 || offset > lastOffset) {
                debug("Adding index entry %d => %d to %s.".format(offset.toString(), position.toString(), file.getName()));
                this.mmap.putInt((int) (offset - baseOffset));
                this.mmap.putInt(position);
                this.size.incrementAndGet();
                this.lastOffset = offset;
                Prediction.require(entries() * 8 == mmap.position(), entries() + " entries but file position in index is " + mmap.position() + ".");
            } else {
                throw new InvalidOffsetException("Attempt to append an offset (%d) to position %d no larger than the last offset appended (%d) to %s."
                        .format(offset.toString(), entries().toString(), lastOffset, file.getAbsolutePath()));
            }
        });
    }

    /**
     * True iff there are no more slots available in this index
     */
    public boolean isFull() {
        return entries() >= this.maxEntries;
    }

    /**
     * Truncate the entire index, deleting all entries
     */
    public void truncate() {
        truncateToEntries(0);
    }

    /**
     * Remove all entries from the index which have an offset greater than or equal to the given offset.
     * Truncating to an offset larger than the largest in the index has no effect.
     */
    public void truncateTo(Long offset) {
        Utils.inLock(lock, () -> {
            ByteBuffer idx = mmap.duplicate();
            int slot = indexSlotFor(idx, offset);

      /* There are 3 cases for choosing the new size
       * 1) if there is no entry in the index <= the offset, delete everything
       * 2) if there is an entry for this exact offset, delete it and everything larger than it
       * 3) if there is no entry for this offset, delete everything larger than the next smallest
       */
            int newEntries;
            if (slot < 0)
                newEntries = 0;
            else if (relativeOffset(idx, slot) == offset - baseOffset)
                newEntries = slot;
            else
                newEntries = slot + 1;
            truncateToEntries(newEntries);
        });
    }

    /**
     * Truncates index to a known number of entries.
     */
    private void truncateToEntries(Integer entries) {
        Utils.inLock(lock, () -> {
            this.size.set(entries);
            mmap.position(this.size.get() * 8);
            this.lastOffset = readLastEntry().offset;
        });
    }

    /**
     * Trim this segment to fit just the valid entries, deleting all trailing unwritten bytes from
     * the file.
     */
    public void trimToValidSize() {
        Utils.inLock(lock, () -> resize(entries() * 8));
    }

    /**
     * Reset the size of the memory map and the underneath file. This is used in two kinds of cases: (1) in
     * trimToValidSize() which is called at closing the segment or new segment being rolled; (2) at
     * loading segments from disk or truncating back to an old segment where a new log segment became active;
     * we want to reset the index size to maximum index size to avoid rolling new segment.
     */
    public void resize(Integer newSize) {
        Utils.inLock(lock, () -> {
            final RandomAccessFile raf;
            try {
                raf = new RandomAccessFile(file, "rws");

                int roundedNewSize = roundToExactMultiple(newSize, 8);
                int position = this.mmap.position();

      /* Windows won't let us modify the file length while the file is mmapped :-( */
                if (Os.isWindows)
                    forceUnmap(this.mmap);
                try {
                    raf.setLength(roundedNewSize);
                    this.mmap = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, roundedNewSize);
                    this.maxEntries = this.mmap.limit() / 8;
                    this.mmap.position(position);
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        raf.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        });
    }

    /**
     * Forcefully free the buffer's mmap. We do this only on windows.
     */
    private void forceUnmap(MappedByteBuffer m) {
        try {
            if (m instanceof sun.nio.ch.DirectBuffer)
                ((DirectBuffer) m).cleaner().clean();
        } catch (Exception e) {
            warn("Error when freeing index buffer", e);
        }
    }

    /**
     * Flush the data in the index to disk
     */
    public void flush() {
        Utils.inLock(lock, () -> mmap.force());
    }

    /**
     * Delete this index file
     */
    public Boolean delete() {
        info("Deleting index " + this.file.getAbsolutePath());
        return this.file.delete();
    }

    /**
     * The number of entries in this index
     */
    public Integer entries() {
        return size.get();
    }

    /**
     * The number of bytes actually used by this index
     */
    public Integer sizeInBytes() {
        return 8 * entries();
    }

    /**
     * Close the index
     */
    public void close() {
        trimToValidSize();
    }

    /**
     * Rename the file that backs this offset index
     *
     * @return true iff the rename was successful
     */
    public Boolean renameTo(File f) {
        Boolean success = this.file.renameTo(f);
        this.file = f;
        return success;
    }

    /**
     * Do a basic sanity check on this index to detect obvious problems
     *
     * @throws IllegalArgumentException if any problems are found
     */
    public void sanityCheck() {
        Prediction.require(entries() == 0 || lastOffset > baseOffset,
                "Corrupt index found, index file (%s) has non-zero size but the last offset is %d and the base offset is %d"
                        .format(file.getAbsolutePath(), lastOffset, baseOffset));
        Long len = file.length();
        Prediction.require(len % 8 == 0,
                "Index file " + file.getName() + " is corrupt, found " + len +
                        " bytes which is not positive or not a multiple of 8.");
    }

    /**
     * Round a number to the greatest exact multiple of the given factor less than the given number.
     * E.g. roundToExactMultiple(67, 8) == 64
     */
    private Integer roundToExactMultiple(Integer number, Integer factor) {
        return factor * (number / factor);
    }

    /**
     * Execute the given function in a lock only if we are running on windows. We do this
     * because Windows won't let us resize a file while it is mmapped. As a result we have to force unmap it
     * and this requires synchronizing reads.
     */
    private <T> T maybeLock(Lock lock, Processor<T> process) {
        if (Os.isWindows)
            lock.lock();
        try {
            return process.process();
        } finally {
            if (Os.isWindows)
                lock.unlock();
        }
    }

}