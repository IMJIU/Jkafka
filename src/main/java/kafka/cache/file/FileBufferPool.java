package kafka.cache.file;/**
 * Created by zhoulf on 2017/5/9.
 */

import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.NumberFormat;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author
 * @create 2017-05-09 16:39
 **/
public class FileBufferPool {
    public static Logger logger = LoggerFactory.getLogger(FileBufferPool.class.getName());
    private ReentrantLock lock = new ReentrantLock();
    public TreeSet<FileBuffer> fileList = Sets.newTreeSet();
    public static final String DIR = "d:/file_cache/";
    public static final String FileSuffix = ".cache";
    public static final int MAX_FILE_LENGTH = 1024 * 1024 * 50;
    public FileBuffer current;

    /**
     * 初始化
     *
     * @throws IOException
     */
    public FileBufferPool() throws IOException {
        File d = new File(DIR);
        d.mkdirs();
        for (File f : d.listFiles()) {
            Long relPos = Long.parseLong(f.getName().substring(0, f.getName().length() - FileSuffix.length()));
            fileList.add(new FileBuffer(f, openChannel(f, true), relPos.intValue(), Integer.MAX_VALUE, false));
        }
        if (current == null) {
            if (CollectionUtils.isNotEmpty(fileList)) {
                current = fileList.last();
            } else {
                current = addNewCacheFile(new File(DIR + filenamePrefixFromOffset(0L) + FileSuffix), 0);
            }
        }
    }

    /**
     * 增加消息缓存
     *
     * @param buffer
     * @return
     * @throws IOException
     */
    public FileBuffer append(ByteBuffer buffer) throws IOException {
        lock.lock();
        if (current.size() + buffer.limit() > MAX_FILE_LENGTH) {
            Long relPos = Long.parseLong(current.file.getName().substring(0, current.file.getName().length() - 6));
//                System.out.println(String.format("size:%d-buf:%d-relpos%d",current.size(),buffer.limit(),relPos));
            String start = filenamePrefixFromOffset(relPos + current.size());
            current = addNewCacheFile(new File(DIR + start + ".cache"), Integer.parseInt(start));
        }
        Integer start = current.size();
        current.write(buffer);
        Integer end = current.size();
        lock.unlock();
        return new FileBuffer(current.file, openChannel(current.file, false), start, end, true);
    }

    /**
     * 增加新的缓存文件
     * @param file
     * @param start
     * @return
     * @throws IOException
     */
    private FileBuffer addNewCacheFile(File file, Integer start) throws IOException {
        if (!file.exists()) {
            file.createNewFile();
        }
        FileBuffer item = new FileBuffer(file, openChannel(file, true), start, Integer.MAX_VALUE, false);
        fileList.add(item);
        return item;
    }

    /**
     * Open a channel for the given file
     */
    public static FileChannel openChannel(File file, Boolean mutable) throws FileNotFoundException {
        if (mutable)
            return new RandomAccessFile(file, "rw").getChannel();
        else
            return new FileInputStream(file).getChannel();
    }

    public static String filenamePrefixFromOffset(Long offset) {
        NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(20);
        nf.setMaximumFractionDigits(0);
        nf.setGroupingUsed(false);
        return nf.format(offset);
    }

}
