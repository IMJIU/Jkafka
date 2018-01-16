package kafka.utils;

import org.junit.Test;

import java.io.IOException;

/**
 * Created by Administrator on 2017/4/4.
 */
public class TestZKUtils {
    public static String zookeeperConnect;

    public static String zookeeperConnect() {
        if (zookeeperConnect != null) {
            return zookeeperConnect;
        }
        zookeeperConnect = "127.0.0.1:" + TestUtils.choosePort();
        return zookeeperConnect;
    }

}
