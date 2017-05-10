package kafka.cache;/**
 * Created by zhoulf on 2017/5/9.
 */

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import kafka.log.Log;
import kafka.utils.Logging;
import kafka.utils.Utils;
import org.apache.commons.collections.CollectionUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author
 * @create 2017-05-09 16:39
 **/
public class FileCacheMgr extends Logging {
    private Object lock = new Object();
    private ReentrantLock lock2 = new ReentrantLock();
    public TreeSet<FileCacheItem> fileList = Sets.newTreeSet();
    public static final String dir = "d:/file_cache/";
    public static final int MAX_FILE_LENGTH = 1024 * 1024 * 50;
    public FileCacheItem current;

    public static void main(String[] args) throws IOException, InterruptedException {
//        test_fileName_sort();
        test_read_write_thread();
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

    private static void test_read_write_thread() throws IOException, InterruptedException {
//        FileOutputStream fileOutputStream = new FileInputStream(new File)
        final FileCacheMgr mgr = new FileCacheMgr();
        SocketSendBufferPool pool = new SocketSendBufferPool();
        List<FileCacheItem> items = Lists.newArrayList();
        ExecutorService executors = Executors.newFixedThreadPool(10);
        CountDownLatch latch = new CountDownLatch(10);
        long begin = System.currentTimeMillis();
        for (int t = 0; t < 10; t++) {
            executors.execute(() -> {
                doTest(mgr);
                latch.countDown();
            });
        }
        latch.await();
        executors.shutdown();
        System.out.println(System.currentTimeMillis() - begin);
    }

    private static void test_read_write_single() throws IOException, InterruptedException {
        final FileCacheMgr mgr = new FileCacheMgr();
        SocketSendBufferPool pool = new SocketSendBufferPool();
        List<FileCacheItem> items = Lists.newArrayList();
        long begin = System.currentTimeMillis();
        doTest(mgr);
        System.out.println(System.currentTimeMillis() - begin);
        for (FileCacheItem i : items) {
            System.out.println(new String(i.read().array()));
        }
    }

    private static void doTest(FileCacheMgr mgr) {
        for (int i = 0; i < 10; i++) {
            byte[] bs = ("a============================================" +
                    "================================================================b" +
                    "================================================================b" +
                    "================================================================b" +
                    "================================================================b" +
                    "================================================================b" +
                    "================================================================b" +
                    "================================================================b" +
                    "================================================================b" +
                    "================================================================b" +
                    "================================================================b" +
                    "================================================================b" +
                    "================================================================b" +
                    "================================================================b" +
                    "================================================================b" +
                    "================================================================b" +
                    "================================================================b" +
                    "================================================================b" +
                    "================================================================b" +
                    "================================================================b" +
                    "================================================================b" +
                    "================================================================b" +
                    "================================================================b" +
                    "================================================================b" +
                    "================================================================b" +
                    "================================================================b" +
                    "================================================================b" +
                    "================================================================b" +
                    "================================================================b" +
                    "================================================================b" +
                    "================================================================b" +
                    "================================================================b" + i).getBytes();
            ByteBuffer b = ByteBuffer.wrap(bs);
            try {
                mgr.append(b);
            } catch (IOException e) {
                e.printStackTrace();
            }
//            pool.acquire(bs);
//            items.add(mgr.append(b));
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
        lock2.lock();
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
        lock2.unlock();
        return new FileCacheItem(current.file, Utils.openChannel(current.file, false), start, end, true);
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
