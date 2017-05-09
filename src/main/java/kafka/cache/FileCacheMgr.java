package kafka.cache;/**
 * Created by zhoulf on 2017/5/9.
 */

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import kafka.log.Log;
import kafka.utils.Utils;
import org.apache.commons.collections.CollectionUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

/**
 * @author
 * @create 2017-05-09 16:39
 **/
public class FileCacheMgr {
    public TreeSet<FileCacheItem> fileList = Sets.newTreeSet();
    public static final String dir = "d:/file_cache/";
    public static final int MAX_FILE_LENGTH = 1024;

    public static void main(String[] args) throws IOException {
//        fileName_sort();
        read_write();
    }

    private static void fileName_sort() throws FileNotFoundException {
        TreeSet<FileCacheItem> fileList = Sets.newTreeSet();
        for (int i = 0; i < 100; i++) {
            Long num = (long) (new Random().nextInt(100));
            File file = new File(dir + Log.filenamePrefixFromOffset(num) + ".cache");
            fileList.add(new FileCacheItem(file, Utils.openChannel(file, true), num.intValue(), Integer.MAX_VALUE, false));
        }
        for (FileCacheItem i : fileList) {
            System.out.println(i.file.getName());
        }
    }

    private static void read_write() throws IOException {
        FileCacheMgr mgr = new FileCacheMgr();
        List<FileCacheItem> items = Lists.newArrayList();
        for (int i = 0; i < 100; i++) {
            ByteBuffer b = ByteBuffer.wrap(("a===========b" + i).getBytes());
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
            fileList.add(new FileCacheItem(f, Utils.openChannel(f, true), 0, Integer.MAX_VALUE, false));
        }
    }

    public synchronized FileCacheItem append(ByteBuffer buffer) throws IOException {
        FileCacheItem item;
        if (CollectionUtils.isNotEmpty(fileList)) {
            item = fileList.last();
        } else {
            item = addNew(new File(dir + Log.filenamePrefixFromOffset(0L) + ".cache"));
        }
        Long relPos =  Long.parseLong(item.file.getName().substring(0, item.file.getName().length() - 6));
        if (item.size() + buffer.limit() > MAX_FILE_LENGTH) {
            item = addNew(new File(dir + Log.filenamePrefixFromOffset(relPos+ item.size()) + ".cache"));
        }
        Long start = item.channel.position();
        item.write(buffer);
        Long end = item.channel.position()-1;
        return new FileCacheItem(item.file, Utils.openChannel(item.file, false), start.intValue(), end.intValue(), true);
    }

    private FileCacheItem addNew(File file) throws IOException {
        if (!file.exists()) {
            file.createNewFile();
        }
        FileCacheItem item = new FileCacheItem(file, Utils.openChannel(file, true), 0, Integer.MAX_VALUE, false);
        fileList.add(item);
        return item;
    }
}
