package kafka.network;

import kafka.common.KafkaException;
import kafka.utils.Logging;

import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ReadableByteChannel;

/**
 * Represents a stateful transfer of data to or from the network
 */
public abstract class Transmission extends Logging {

    public abstract boolean complete();

    protected void expectIncomplete() {
        if (complete())
            throw new KafkaException("This operation cannot be completed on a complete request.");
    }

    protected void expectComplete() {
        if (!complete())
            throw new KafkaException("This operation cannot be completed on an incomplete request.");
    }
}


/**
 * A transmission that is being received from a channel
 */
abstract class Receive extends Transmission {

    public abstract ByteBuffer buffer();

    public abstract Integer readFrom(ReadableByteChannel channel);

    public Integer readCompletely(ReadableByteChannel channel) {
        Integer totalRead = 0;
        while (!complete()) {
            Integer read = readFrom(channel);
            trace(read + " bytes read.");
            totalRead += read;
        }
        return totalRead;
    }

}

/**
 * A transmission that is being sent out to the channel
 */
abstract class Send extends Transmission {

    public abstract Integer writeTo(GatheringByteChannel channel);

    public Integer  writeCompletely(GatheringByteChannel channel) {
        Integer totalWritten = 0;
        while (!complete()) {
            Integer written = writeTo(channel);
            trace(written + " bytes written.");
            totalWritten += written;
        }
        return totalWritten;
    }

}
