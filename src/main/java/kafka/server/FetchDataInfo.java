package kafka.server;

import kafka.message.MessageSet;

/**
 * Created by Administrator on 2017/3/29.
 */
public class FetchDataInfo {
    public LogOffsetMetadata fetchOffset;
    public MessageSet messageSet;

    public FetchDataInfo(LogOffsetMetadata fetchOffset, MessageSet messageSet) {
        this.fetchOffset = fetchOffset;
        this.messageSet = messageSet;
    }
}
