package kafka.javaapi;

import kafka.common.OffsetMetadataAndError;
import kafka.log.TopicAndPartition;

import java.util.Map;

/**
 * @author zhoulf
 * @create 2017-12-19 17 20
 **/
public class OffsetFetchResponse {
    private kafka.api.OffsetFetchResponse underlying;

    public OffsetFetchResponse(kafka.api.OffsetFetchResponse underlying) {
        this.underlying = underlying;
        offsets = underlying.requestInfo;
    }

    public Map<TopicAndPartition, OffsetMetadataAndError> offsets;

}
