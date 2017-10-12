package kafka.network;

import java.nio.channels.GatheringByteChannel;


public abstract class Send extends Transmission {

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
