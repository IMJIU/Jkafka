package kafka.network;

/**
 * Created by Administrator on 2017/4/22.
 */
public class InvalidRequestException extends RuntimeException {
    public InvalidRequestException(String msg) {
        super(msg);
    }
}