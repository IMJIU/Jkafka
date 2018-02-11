package kafka.network;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/**
 * @author zhoulf
 * @create 2017-10-27 17:18
 **/
public
/**
 * A transmission that is being received from a channel
 */
abstract class Receive extends Transmission {

    public abstract ByteBuffer buffer();

    public abstract Integer readFrom(ReadableByteChannel channel) throws IOException;

    public Integer readCompletely(ReadableByteChannel channel) throws IOException {
        Integer totalRead = 0;
        while (!complete()) {
            Integer read = readFrom(channel);
            trace(read + " bytes read.");
            totalRead += read;
        }
        return totalRead;
    }

}

