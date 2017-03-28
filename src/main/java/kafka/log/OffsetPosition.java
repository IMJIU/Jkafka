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

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof OffsetPosition) {
            OffsetPosition other = (OffsetPosition) obj;
            if (position.equals(other.position) && offset.equals(other.offset)) {
                return true;
            }
        }
        return false;
    }
}
