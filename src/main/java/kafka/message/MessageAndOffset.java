package kafka.message;/**
 * Created by zhoulf on 2017/3/23.
 */

/**
 * @author
 * @create 2017-03-23 12:44
 **/
public class MessageAndOffset {

    public final Message message;

    public final long offset;

    public MessageAndOffset(Message message, long offset) {
        this.message = message;
        this.offset = offset;
    }

    @Override
    public String toString() {
        return String.format("MessageAndOffset [offset=%s, message=%s]", offset, message);
    }
}
