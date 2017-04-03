package kafka.test;/**
 * Created by zhoulf on 2017/3/30.
 */

import com.google.common.collect.Lists;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * @author
 * @create 2017-03-30 10:04
 **/
public class T01 {
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
                main + "utils/Scheduler.java",
                main + "log/LogConfig.java");
//        List<String> filePaths = Arrays.asList(main + "log/Log.java");
        for (String p : filePaths) {
            convertToJava(p, true);
        }
    }


    public static void convertToJava(String filePath, boolean write) throws IOException {
        List<Character> lastList = Lists.newArrayList('[', '(', '}', '{', ';', '*', '/', ',', '>', '=');
        String p = "(\\w+):\\s?(\\w+)";
        String p1 = "[\\s\\(](Int)\\s";
        String p2 = "public(.+):\\s?(\\w+)\\s?=";
        String r2 = "public $2 $1";
        String p4 = "(\".+\")\\.format\\(";
        String r4 = "String.format($1,";
        String p5 = "for\\((\\w+)\\s?\\<-\\s?(\\d+)\\s+until\\s+(\\d+)\\)";
        String r5 = "for(int $1 = $2; $1 < $3; $1++)";
        String p6 = "\\[(\\S+\\s?\\S+)\\]";
        String r6 = "<$1>";
        StringBuilder content = new StringBuilder();
        boolean comment = false;

        BufferedReader reader = new BufferedReader(new FileReader(filePath));
        String s;
        while ((s = reader.readLine()) != null) {
            if (s.length() > 0) {
                if (s.contains(" assertEquals")) {
                    s = s.replaceAll("assertEquals", "Assert.assertEquals");
                }
                s = s.replaceAll(p2, r2);
                if (s.contains(" def ")) {
                    s = s.replaceAll("def", "public void ");
                }
                content.append(s.replaceAll(p, "$2 $1")
                        .replaceAll(p4, r4)
                        .replaceAll(p1, " Integer ")
                        .replaceAll(p5, r5)
                        .replaceAll(p6, r6)
                );
                if (s.indexOf("/*") != -1) {
                    comment = true;
                }
                if (s.indexOf("*/") != -1) {
                    comment = false;
                }
                if (!comment) {
                    addDelimit(lastList, content, s);
                }
            }
            content.append("\n");
        }
        System.out.println(content.toString());
        if (write) {
            FileWriter writer = new FileWriter(filePath);
            writer.write(content.toString());
            writer.flush();
        }
    }

    private static void addDelimit(List<Character> lastList, StringBuilder content, String s) {
        char last = s.charAt(s.length() - 1);
        boolean inLast = false;
        for (Character c : lastList) {
            if (c == last) {
                inLast = true;
                break;
            }
        }
        if (!inLast) {
            if (!(last == ')' && (s.contains("if") || s.contains("for"))) && !s.contains("@")) {
                content.append(";");
            }
        }
    }
}
