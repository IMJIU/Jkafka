package kafka.api;

import kafka.common.ErrorMapping;

import java.util.List;

/**
 * @author zhoulf
 * @create 2017-10-30 17:02
 **/
public class PartitionOffsetsResponse {
    public Short error;
    public List<Long> offsets;

    public PartitionOffsetsResponse(Short error, List<Long> offsets) {
        this.error = error;
        this.offsets = offsets;
    }

    @Override
    public String toString() {
        return new String("error: " + ErrorMapping.exceptionFor(error).getClass().getName() + " offsets: " + offsets);
    }
}