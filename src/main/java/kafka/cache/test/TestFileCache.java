package kafka.cache.test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import kafka.cache.file.FileBuffer;
import kafka.cache.file.FileBufferMgr;
import kafka.cache.mem.SocketSendBufferPool;
import kafka.log.Log;
import kafka.utils.Utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author zhoulf
 * @create 2017-10-31 9:36
 **/
public class TestFileCache {

    public static void main(String[] args) throws IOException, InterruptedException {
//        test_fileName_sort();

    }

    private static void fileName_sort() throws FileNotFoundException {
        TreeSet<FileBuffer> fileList = Sets.newTreeSet();
        for (int i = 0; i < 1000; i++) {
            Long num = (long) (new Random().nextInt(100));
            File file = new File(FileBufferMgr.DIR + Log.filenamePrefixFromOffset(num) + FileBufferMgr.FileSuffix);
            fileList.add(new FileBuffer(file, Utils.openChannel(file, true), num.intValue(), Integer.MAX_VALUE, false));
        }
        for (FileBuffer i : fileList) {
            System.out.println(i.file.getName());
        }
    }

    private static void read_write_thread() throws IOException, InterruptedException {
        final FileBufferMgr mgr = new FileBufferMgr();
        SocketSendBufferPool pool = new SocketSendBufferPool();
        List<FileBuffer> items = Lists.newArrayList();
        ExecutorService executors = Executors.newFixedThreadPool(10);
        CountDownLatch latch = new CountDownLatch(10);
        long begin = System.currentTimeMillis();
        for (int t = 0; t < 10; t++) {
            executors.execute(() -> {
                try {
                    doTest(mgr, pool);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                latch.countDown();
            });
        }
        latch.await();
        executors.shutdown();
        System.out.println(System.currentTimeMillis() - begin);
    }

    private static void test_read_write_single() throws IOException, InterruptedException {
        final FileBufferMgr mgr = new FileBufferMgr();
        SocketSendBufferPool pool = new SocketSendBufferPool();
        List<FileBuffer> items = Lists.newArrayList();
        long begin = System.currentTimeMillis();
        doTest(mgr, pool);
        System.out.println(System.currentTimeMillis() - begin);
        for (FileBuffer i : items) {
            System.out.println(new String(i.read().array()));
        }
    }

    private static void doTest(FileBufferMgr mgr, SocketSendBufferPool pool) throws IOException {
        for (int i = 0; i < 1000; i++) {
            byte[] bs = ("a============================================" + i).getBytes();
            ByteBuffer b = ByteBuffer.wrap(bs);
            mgr.append(b);
//            pool.acquire(bs);
        }
    }

}
