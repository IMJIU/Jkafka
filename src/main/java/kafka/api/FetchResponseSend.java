package kafka.api;

import kafka.network.MultiSend;
import kafka.network.Send;
import kafka.utils.Utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

/**
 * @author zhoulf
 * @create 2017-10-25 18:04
 **/
public class FetchResponseSend extends Send {
    public FetchResponse fetchResponse;
    public Send sends;

    public FetchResponseSend(FetchResponse fetchResponse) {
        this.fetchResponse = fetchResponse;
        size = fetchResponse.sizeInBytes();
        buffer.putInt(size);
        buffer.putInt(fetchResponse.correlationId);
        buffer.putInt(fetchResponse.dataGroupedByTopic.size()); // topic count
        buffer.rewind();
        sends = new MultiSend(Utils.map(fetchResponse.dataGroupedByTopic, (topic, topicAndData) ->
                new TopicDataSend(new TopicData(topic, Utils.mapKey(topicAndData, k -> k.partition))))) {
            public Integer expectedBytesToWrite() {
                return fetchResponse.sizeInBytes() - FetchResponse.headerSize;
            }
        };
    }

    private Integer size;

    private int sent = 0;

    private Integer sendSize = 4 /* for size */ + size;


    private ByteBuffer buffer = ByteBuffer.allocate(4 /* for size */ + FetchResponse.headerSize);


    public Send send;

    @Override
    public Integer writeTo(GatheringByteChannel channel) {
        expectIncomplete();
        int written = 0;
        if (buffer.hasRemaining())
            try {
                written += channel.write(buffer);
            } catch (IOException e) {
                e.printStackTrace();
            }
        if (!buffer.hasRemaining() && !sends.complete()) {
            written += sends.writeTo(channel);
        }
        sent += written;
        return written;
    }


    @Override
    public boolean complete() {
        return sent >= sendSize;
    }
}
