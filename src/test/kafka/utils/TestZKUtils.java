package kafka.utils;

import java.io.IOException;

/**
 * Created by Administrator on 2017/4/4.
 */
public class TestZKUtils {
    public static String zookeeperConnect;
    public static String zookeeperConnect(){
        if(zookeeperConnect!=null){
            return zookeeperConnect;
        }
        try {
            zookeeperConnect = "127.0.0.1:" + TestUtils.choosePort();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return zookeeperConnect;
    }
}
