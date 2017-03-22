package kafka.message;/**
 * Created by zhoulf on 2017/3/22.
 */

import java.nio.ByteBuffer;

/**
 * @author
 * @create 2017-03-22 20:00
 **/
public abstract  class MessageSet {
    public static int MessageSizeLength = 4;
    public static int OffsetLength = 8;
    public static int LogOverhead = MessageSizeLength + OffsetLength;
    public MessageSet Empty = new ByteBufferMessageSet(ByteBuffer.allocate(0));

}
