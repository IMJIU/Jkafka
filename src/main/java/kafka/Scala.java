package kafka;/**
 * Created by zhoulf on 2017/3/22.
 */

import com.google.common.collect.Lists;
import kafka.test.T01;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * @author
 * @create 2017-03-22 20:37
 **/
public class Scala {
    public static final String main = "src/main/java/kafka/";
    public static final String test = "src/test/";
    public static void main(String[] args) throws IOException {
//        test_format();
//        String filePath = "src/main/java/kafka/log/LogSegment.java";
//        String filePath = main + "utils/Utils.java";
//        String f = "for(i <- 1 until 10)";
//        System.out.println(f.replaceAll("for\\((\\w+)\\s?\\<-\\s?(\\d+)\\s+until\\s+(\\d+)\\)","for(int $1 = $2; $1 < $3; $1++)"));
        List<String> filePaths = Arrays.asList(
//                main + "server/ReplicaManager.java",
//                main + "cluster/Partition.java",
                main + "server/BrokerTopicStat.java",
                main + "log/LogConfig.java");
//        List<String> filePaths = Arrays.asList(main + "log/Log.java");
        for (String p : filePaths) {
            T01.convertToJava(p, true);
        }
    }
}
