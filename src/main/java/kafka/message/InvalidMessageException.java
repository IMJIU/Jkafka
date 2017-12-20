package kafka.message;/**
 * Created by zhoulf on 2017/3/22.
 */

/**
 * @author
 * @create 2017-03-22 20:39
 **/
public class InvalidMessageException extends RuntimeException {
    public InvalidMessageException(String message) {
        super(message);
    }

    public InvalidMessageException() {
        super();
    }
}
