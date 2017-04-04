package kafka.utils;

import java.io.IOException;

/**
 * Created by Administrator on 2017/4/4.
 */
public class TestZKUtils {
    public static String zookeeperConnect;

    static {
        try {
            zookeeperConnect = "127.0.0.1:" + TestUtils.choosePort();
        } catch (IOException e) {
            e.printStackTrace();
        }
        zookeeperConnect = null;
    }
}
