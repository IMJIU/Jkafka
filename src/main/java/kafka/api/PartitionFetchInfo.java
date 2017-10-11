package kafka.api;

/**
 * @author zhoulf
 * @create 2017-10-11 11:24
 **/
public class PartitionFetchInfo {
    public Long offset;
    public Integer fetchSize;

    public PartitionFetchInfo(Long offset, Integer fetchSize) {
        this.offset = offset;
        this.fetchSize = fetchSize;
    }
}
