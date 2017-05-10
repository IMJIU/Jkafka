package kafka.cache;/**
 * Created by zhoulf on 2017/5/9.
 */

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import kafka.log.Log;
import kafka.utils.Logging;
import kafka.utils.SystemTime;
import kafka.utils.Utils;
import org.apache.commons.collections.CollectionUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

/**
 * @author
 * @create 2017-05-09 16:39
 **/
public class FileCacheMgr extends Logging {
    private Object lock = new Object();
    public TreeSet<FileCacheItem> fileList = Sets.newTreeSet();
    public static final String dir = "d:/file_cache/";
    public static final int MAX_FILE_LENGTH = 1024 * 1024 * 50;
    public FileCacheItem current;
    public int currentPos = 0;

    public static void main(String[] args) throws IOException {
//        test_fileName_sort();
        long begin = System.currentTimeMillis();
        test_read_write();
        System.out.println(System.currentTimeMillis() - begin);
    }

    private static void test_fileName_sort() throws FileNotFoundException {
        TreeSet<FileCacheItem> fileList = Sets.newTreeSet();
        for (int i = 0; i < 1000; i++) {
            Long num = (long) (new Random().nextInt(100));
            File file = new File(dir + Log.filenamePrefixFromOffset(num) + ".cache");
            fileList.add(new FileCacheItem(file, Utils.openChannel(file, true), num.intValue(), Integer.MAX_VALUE, false));
        }
        for (FileCacheItem i : fileList) {
            System.out.println(i.file.getName());
        }
    }

    private static void test_read_write() throws IOException {
//        FileOutputStream fileOutputStream = new FileInputStream(new File)
        FileCacheMgr mgr = new FileCacheMgr();
        SocketSendBufferPool pool = new SocketSendBufferPool();
        List<FileCacheItem> items = Lists.newArrayList();

        for (int i = 0; i < 100; i++) {
            byte[]bs = ("a============================================" +
//                    "================================================================b"+
//                    "================================================================b"+
//                    "================================================================b"+
//                    "================================================================b"+
//                    "================================================================b"+
//                    "================================================================b"+
//                    "================================================================b"+
//                    "================================================================b"+
//                    "================================================================b"+
//                    "================================================================b"+
//                    "================================================================b"+
//                    "================================================================b"+
//                    "================================================================b"+
//                    "================================================================b"+
//                    "================================================================b"+
//                    "================================================================b"+
//                    "================================================================b"+
//                    "================================================================b"+
//                    "================================================================b"+
//                    "================================================================b"+
//                    "================================================================b"+
//                    "================================================================b"+
//                    "================================================================b"+
//                    "================================================================b"+
//                    "================================================================b"+
//                    "================================================================b"+
//                    "================================================================b"+
//                    "================================================================b"+
//                    "================================================================b"+
//                    "================================================================b"+
                    "================================================================b" + i).getBytes();
            ByteBuffer b = ByteBuffer.wrap(bs);
            mgr.append(b);
//            pool.acquire(bs);
            items.add(mgr.append(b));
        }
        for (FileCacheItem i : items) {
            System.out.println(new String(i.read().array()));
        }
    }

    public FileCacheMgr() throws FileNotFoundException {
        File d = new File(dir);
        d.mkdirs();
        for (File f : d.listFiles()) {
            Long relPos = Long.parseLong(f.getName().substring(0, f.getName().length() - 6));
            fileList.add(new FileCacheItem(f, Utils.openChannel(f, true), relPos.intValue(), Integer.MAX_VALUE, false));
        }
    }

    public FileCacheItem append(ByteBuffer buffer) throws IOException {
        synchronized (lock) {
            if (current == null) {
                if (CollectionUtils.isNotEmpty(fileList)) {
                    current = fileList.last();
                } else {
                    current = addNew(new File(dir + Log.filenamePrefixFromOffset(0L) + ".cache"), 0);
                }
            }
            if (current.size() + buffer.limit() > MAX_FILE_LENGTH) {
                Long relPos = Long.parseLong(current.file.getName().substring(0, current.file.getName().length() - 6));
//                System.out.println(String.format("size:%d-buf:%d-relpos%d",current.size(),buffer.limit(),relPos));
                String start = Log.filenamePrefixFromOffset(relPos + current.size());
                current = addNew(new File(dir + start + ".cache"), Integer.parseInt(start));
            }
            Integer start = current.size();
            current.write(buffer);
            Integer end = current.size();
            return new FileCacheItem(current.file, Utils.openChannel(current.file, false), start, end, true);
        }
    }

    private FileCacheItem addNew(File file, Integer start) throws IOException {
        if (!file.exists()) {
            file.createNewFile();
        }
        FileCacheItem item = new FileCacheItem(file, Utils.openChannel(file, true), start, Integer.MAX_VALUE, false);
        fileList.add(item);
        return item;
    }
}
