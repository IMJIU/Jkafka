package kafka.common;/**
 * Created by zhoulf on 2017/4/11.
 */

/**
 * @author
 * @create 2017-04-11 13:48
 **/
public class LogCleaningAbortedException  extends RuntimeException {
    public LogCleaningAbortedException(String msg) {
        super(msg);
    }
}
