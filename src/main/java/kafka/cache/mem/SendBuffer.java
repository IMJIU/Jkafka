package kafka.cache.mem;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.DatagramChannel;
import java.nio.channels.WritableByteChannel;

/**
 * @author zhoulf
 * @create 2017-10-31 9:49
 **/
public interface SendBuffer {
    boolean finished();

    long writtenBytes();

    long totalBytes();

    long transferTo(WritableByteChannel ch) throws IOException;

    long transferTo(DatagramChannel ch, SocketAddress raddr) throws IOException;

    void release();
}
