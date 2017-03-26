package kafka.message;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.util.Iterator;

/**
 * Created by Administrator on 2017/3/26.
 */
public class FileMessageSet extends MessageSet {
    public FileMessageSet(File file, FileChannel channel) {

    }

    public Integer writeTo(GatheringByteChannel channel, Long offset, Integer maxSize) throws IOException {
        return null;
    }

    public Iterator<MessageAndOffset> iterator() {
        return null;
    }


    public Integer sizeInBytes() {
        return null;
    }
}
