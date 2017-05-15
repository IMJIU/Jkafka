package kafka.utils;/**
 * Created by zhoulf on 2017/3/29.
 */

/**
 * @author
 * @create 2017-03-29 12:57
 **/
public class Prediction {

    public static void require(Boolean check, String msg) {
        if (!check) {
            throw new IllegalArgumentException(msg);
        }
    }
    public static void Assert(Boolean check, String msg) {
        if (!check) {
            throw new IllegalArgumentException(msg);
        }
    }
    public static void require(Boolean check) {
        if (!check) {
            throw new IllegalArgumentException();
        }
    }
}
