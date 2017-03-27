package kafka.log;

/**
 * Created by Administrator on 2017/3/27.
 */
public class OffsetPosition {
    public Long offset;
    public Integer position;

    public OffsetPosition(Long offset, Integer position) {
        this.offset = offset;
        this.position = position;
    }
}
