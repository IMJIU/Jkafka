package kafka.cache;


import kafka.cache.file.FileBuffer;
import kafka.cache.mem.SendBuffer;
import kafka.network.Send;

import java.io.IOException;
import java.nio.channels.GatheringByteChannel;

/**
 * @author zhoulf
 * @create 2017-10-31 13:46
 **/
public class MsgBufferSend extends Send {
    public String ip;
    public FileBuffer fileBuffer;
    public SendBuffer memBuffer;
    private int type;//0mem 1 file
    private boolean complete = false;

    public MsgBufferSend(String ip, FileBuffer fileBuffer) {
        this.ip = ip;
        this.fileBuffer = fileBuffer;
        type = 1;
    }

    public MsgBufferSend(String ip, SendBuffer memBuffer) {
        this.ip = ip;
        this.memBuffer = memBuffer;
        type = 0;
    }

    @Override
    public Integer writeTo(GatheringByteChannel channel) throws IOException {
        long len;
        if (type == 0) {
            len = memBuffer.transferTo(channel);
        } else {
            len = fileBuffer.transferTo(channel);
        }
        complete = true;
        return (int)len;
    }

    @Override
    public boolean complete() {
        return complete;
    }
}
