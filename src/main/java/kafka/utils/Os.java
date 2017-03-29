package kafka.utils;/**
 * Created by zhoulf on 2017/3/29.
 */

/**
 * @author
 * @create 2017-03-29 11:32
 **/
public class Os {
    public static String name = System.getProperty("os.name").toLowerCase();
    public static boolean isWindows = name.startsWith("windows");
}
