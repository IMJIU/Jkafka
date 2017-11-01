package kafka.network;

import java.io.IOException;
import java.nio.channels.GatheringByteChannel;


public abstract class Send extends Transmission {

    public abstract Integer writeTo(GatheringByteChannel channel) throws IOException;

    public Integer  writeCompletely(GatheringByteChannel channel)throws IOException {
        Integer totalWritten = 0;
        while (!complete()) {
            Integer written = writeTo(channel);
            trace(written + " bytes written.");
            totalWritten += written;
        }
        return totalWritten;
    }

}
