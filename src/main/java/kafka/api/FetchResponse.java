package kafka.api;

import kafka.common.ErrorMapping;
import kafka.func.IntCount;
import kafka.func.Tuple;
import kafka.log.TopicAndPartition;
import kafka.message.ByteBufferMessageSet;
import kafka.network.Send;
import kafka.utils.Utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.List;
import java.util.Map;

import static kafka.api.ApiUtils.*;

/*
  * FetchResponse uses <sendfile>(http://man7.org/linux/man-pages/man2/sendfile.2.html)
  * api for data transfer, so `writeTo` aren't actually being used.
  * It is implemented as an empty function to comform to `RequestOrResponse.writeTo`
  * abstract method signature.
  */
public class FetchResponse extends RequestOrResponse {
    public Integer correlationId;
    public Map<TopicAndPartition, FetchResponsePartitionData> data;
    public static final int headerSize =
            4 + /* correlationId */
                    4;/* topic count */
    /**
     * Partitions the data into a map of maps (one for each topic).
     */
    public Map<String, Map<TopicAndPartition, FetchResponsePartitionData>> dataGroupedByTopic;//lazy

    public FetchResponse(Integer correlationId, Map<TopicAndPartition, FetchResponsePartitionData> data) {
        this.correlationId = correlationId;
        this.data = data;
        dataGroupedByTopic = Utils.groupByKey(data, key -> key.topic);
    }

    public FetchResponse readFrom(ByteBuffer buffer) {
        int correlationId = buffer.getInt();
        int topicCount = buffer.getInt();
        List<Tuple<TopicAndPartition, FetchResponsePartitionData>> pairs = Utils.iterateFlat(1, topicCount, n -> {
            TopicData topicData = TopicData.readFrom(buffer);
            return topicData.partitionData.entrySet().stream().map(entry -> {
                Integer partitionId = entry.getKey();
                FetchResponsePartitionData partitionData = entry.getValue();
                return Tuple.of(new TopicAndPartition(topicData.topic, partitionId), partitionData);
            });
        });
        return new FetchResponse(correlationId, Utils.toMap(pairs));
    }


    @Override

    public Integer sizeInBytes() {
        IntCount count = IntCount.of(0);
        dataGroupedByTopic.entrySet().forEach(entry ->
                count.add(new TopicData(entry.getKey(), Utils.mapKey(entry.getValue(), k -> k.partition)).sizeInBytes())
        );
        return FetchResponse.headerSize + count.get();
    }

    public void writeTo(ByteBuffer buffer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String describe(Boolean details) {
        return this.toString();
    }

    private FetchResponsePartitionData partitionDataFor(String topic, Integer partition) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        FetchResponsePartitionData partitionData = data.get(topicAndPartition);
        if (partitionData != null) {
            return partitionData;
        } else {
            throw new IllegalArgumentException(
                    String.format("No partition %s in fetch response %s", topicAndPartition, this.toString()));
        }
    }

    public ByteBufferMessageSet messageSet(String topic, Integer partition) {
        return (ByteBufferMessageSet) partitionDataFor(topic, partition).messages;
    }


    public long highWatermark(String topic, Integer partition) {
        return partitionDataFor(topic, partition).hw;
    }


    public boolean hasError() {
        return Utils.exists(data.values(), e -> e.error != ErrorMapping.NoError);
    }

    public short errorCode(String topic, Integer partition) {
        return partitionDataFor(topic, partition).error;
    }

}

class TopicData {
    public String topic;
    public Map<Integer, FetchResponsePartitionData> partitionData;

    public TopicData(String topic, Map<Integer, FetchResponsePartitionData> partitionData) {
        this.topic = topic;
        this.partitionData = partitionData;
    }

    public static TopicData readFrom(ByteBuffer buffer) {
        String topic = readShortString(buffer);
        int partitionCount = buffer.getInt();
        List<Tuple<Integer, FetchResponsePartitionData>> topicPartitionDataPairs = Utils.iterate(1, partitionCount, n -> {
            int partitionId = buffer.getInt();
            FetchResponsePartitionData partitionData = FetchResponsePartitionData.readFrom(buffer);
            return Tuple.of(partitionId, partitionData);   /* pData : 4(error)+8(hw)+4(msize)+n(data) */
        });
        return new TopicData(topic, Utils.toMap(topicPartitionDataPairs));
    }

    public static int headerSize(String topic) {
        return shortStringLength(topic) +
                4;/* partition count */
    }


    public int sizeInBytes() {
        IntCount count = IntCount.of(0);
        count.add(TopicData.headerSize(topic));
        partitionData.values().forEach(e -> count.add(e.sizeInBytes + 4));
        return count.get();
    }

    public int headerSize = TopicData.headerSize(topic);
}


class FetchResponseSend extends Send {
    public FetchResponse fetchResponse;

    public FetchResponseSend(FetchResponse fetchResponse) {
        this.fetchResponse = fetchResponse;
        size = fetchResponse.sizeInBytes();
        buffer.putInt(size);
        buffer.putInt(fetchResponse.correlationId);
        buffer.putInt(fetchResponse.dataGroupedByTopic.size()); // topic count
        buffer.rewind();

        sends = new MultiSend(fetchResponse.dataGroupedByTopic.toList.map {
        case(topic,data)=>new

            TopicDataSend(TopicData(topic,
                          data.map {
                case (topicAndPartition,message) =>(topicAndPartition.partition, message)}))
        })

        {
            val expectedBytesToWrite = fetchResponse.sizeInBytes - FetchResponse.headerSize
        }
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