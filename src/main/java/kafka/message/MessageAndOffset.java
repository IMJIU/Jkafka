package kafka.message;/**
 * Created by zhoulf on 2017/3/23.
 */

/**
 * @author
 * @create 2017-03-23 12:44
 **/
public class MessageAndOffset {

    public final Message message;

    public final Long offset;

    public MessageAndOffset(Message message, Long offset) {
        this.message = message;
        this.offset = offset;
    }

    public Long nextOffset(){
        return offset + 1;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof MessageAndOffset) {
            MessageAndOffset m = ((MessageAndOffset) obj);
            if (m.offset.equals(offset) && m.message.equals(message)) {
                return true;
            }
            return false;
        }
        return super.equals(obj);
    }

    @Override
    public String toString() {
        return String.format("MessageAndOffset [offset=%s, message=%s]", offset, message);
    }

    @Override
    public int hashCode() {
        int result = message != null ? message.hashCode() : 0;
        result = 31 * result + (offset != null ? offset.hashCode() : 0);
        return result;
    }
}
