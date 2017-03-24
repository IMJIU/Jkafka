package kafka.message;/**
 * Created by zhoulf on 2017/3/24.
 */

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * @author
 * @create 2017-03-24 12:53
 **/
public class ByteBufferBackedInputStream extends InputStream{
    public ByteBuffer buffer;
    public ByteBufferBackedInputStream(ByteBuffer buffer){
        this.buffer = buffer;
    }
    @Override
    public int read() throws IOException {
        if(buffer.hasRemaining()){
            return (buffer.get() & 0xFF);
        }
        return -1;
    }

    @Override
    public int read(byte[] bytes, int off, int len) throws IOException {
        if(buffer.hasRemaining()){
            int realLen = Math.min(len, buffer.remaining());
            buffer.get(bytes, off, realLen);
            return realLen;
        }
        return -1;
    }
}
