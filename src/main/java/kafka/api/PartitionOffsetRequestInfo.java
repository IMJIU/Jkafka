package kafka.api;

/**
 * @author zhoulf
 * @create 2017-10-30 15:54
 **/
public class PartitionOffsetRequestInfo {
    public Long time;
    public Integer maxNumOffsets;

    public PartitionOffsetRequestInfo(Long time, Integer maxNumOffsets) {
        this.time = time;
        this.maxNumOffsets = maxNumOffsets;
    }
}
