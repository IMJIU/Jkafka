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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PartitionFetchInfo that = (PartitionFetchInfo) o;

        if (offset != null ? !offset.equals(that.offset) : that.offset != null) return false;
        return fetchSize != null ? fetchSize.equals(that.fetchSize) : that.fetchSize == null;

    }

    @Override
    public int hashCode() {
        int result = offset != null ? offset.hashCode() : 0;
        result = 31 * result + (fetchSize != null ? fetchSize.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "PartitionFetchInfo{" +
                "offset=" + offset +
                ", fetchSize=" + fetchSize +
                '}';
    }
}
