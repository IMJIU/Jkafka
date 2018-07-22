package kafka.network;

import java.io.IOException;
import java.nio.channels.GatheringByteChannel;
import java.util.List;

/**
 * A set of composite sends, sent one after another
 */
public abstract class MultiSend<S extends Send> extends Send {
    public abstract Integer expectedBytesToWrite();
    private List<S> current;
    int totalWritten = 0;

    public MultiSend(List<S> sends) {
        this.current = sends;
    }

    /**
     * This method continues to write to the socket buffer till an incomplete
     * write happens. On an incomplete write, it returns to the caller to give it
     * a chance to schedule other work till the buffered write completes.
     */
    public Integer writeTo(GatheringByteChannel channel) throws IOException {
        expectIncomplete();
        int totalWrittenPerCall = 0;
        Boolean sendComplete;
        do {
            if(current.size()==0){
                return 0;
            }
            int written  = current.get(0).writeTo(channel);
            totalWritten += written;
            totalWrittenPerCall += written;
            sendComplete = current.get(0).complete();
            if (sendComplete)
                current.remove(0);
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
