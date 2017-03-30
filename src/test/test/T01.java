package test;/**
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
        List<String> filePaths = Arrays.asList(test + "log/OffsetIndexTest.java", test + "log/LogSegmentTest.java");
        for (String p : filePaths) {
            convertToJava(p, true);
        }
    }


    public static void convertToJava(String filePath, boolean write) throws IOException {
        List<Character> lastList = Lists.newArrayList('[', '(', '}', '{', ';', '*', '/', ',', '>', '=');
        String p = "(\\w+):\\s?(\\w+)";
        String p1 = "[\\s\\(](Int)\\s";
        String p2 = "def(.+):\\s?(\\w+)\\s?=";
//        String p3 = " (val) ";
        String p4 = "(\".+\")\\.format\\(";
        StringBuilder content = new StringBuilder();
        boolean comment = false;

        BufferedReader reader = new BufferedReader(new FileReader(filePath));
        String s;
        while ((s = reader.readLine()) != null) {
            if (s.length() > 0) {
                content.append(s.replaceAll(p1, " Integer ")
                                .replaceAll(p, "$2 $1")
                                .replaceAll(p2, "public $2 $1")
//                        .replaceAll(p3, " Integer ")
                                .replaceAll(p4, "String.format($1,")
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
            if (last == ')' && s.indexOf("if") != -1) {

            } else {
                content.append(";");
            }
        }
    }
}
