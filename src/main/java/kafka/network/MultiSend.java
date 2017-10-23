package kafka.network;

import java.nio.channels.GatheringByteChannel;
import java.util.List;

/**
 * A set of composite sends, sent one after another
 */
public abstract class MultiSend<S extends Send> extends Send {
    public abstract Integer expectedBytesToWrite();
    List<S> sends;
    private List<S> current = sends;
    int totalWritten = 0;

    public MultiSend(List<S> sends) {
        this.sends = sends;
        this.current = sends;
    }

    /**
     * This method continues to write to the socket buffer till an incomplete
     * write happens. On an incomplete write, it returns to the caller to give it
     * a chance to schedule other work till the buffered write completes.
     */
    public Integer writeTo(GatheringByteChannel channel) {
        expectIncomplete();
        int totalWrittenPerCall = 0;
        Boolean sendComplete = false;
        do {
            int written = current.get(0).writeTo(channel);
            totalWritten += written;
            totalWrittenPerCall += written;
            sendComplete = current.get(0).complete();
            if (sendComplete)
                current = current.subList(1,current.size());
        } while (!complete() && sendComplete);
        trace("Bytes written as part of multisend call : " + totalWrittenPerCall + "Total bytes written so far : " + totalWritten + "Expected bytes to write : " + expectedBytesToWrite());
        return totalWrittenPerCall;
    }

    public boolean complete() {
        if (current == null) {
            if (totalWritten != expectedBytesToWrite())
                error("mismatch in sending bytes over socket; expected: " + expectedBytesToWrite() + " actual: " + totalWritten);
            return true;
        } else {
            return false;
        }
    }
}