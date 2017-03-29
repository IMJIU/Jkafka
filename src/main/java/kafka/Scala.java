package kafka;/**
 * Created by zhoulf on 2017/3/22.
 */

import com.google.common.collect.Lists;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author
 * @create 2017-03-22 20:37
 **/
public class Scala {
    public static int max(int a, int b) {
        return a > b ? a : b;
    }

    public static byte toByte(int n) {
        return 0;
    }

    public static void main(String[] args) throws IOException {
        List<Character> lastList = Lists.newArrayList('[', '(', '}', '{', ';', '*', '/',',','>','=');
        String filePath = "src/main/java/kafka/log/LogSegment.java";
        String p = "(\\w+):\\s?(\\w+)";
        String p1 = "\\s(Int)\\s";
        String p2 = "def(.+):\\s?(\\w+)\\s?=";
        String p3 = "";
        StringBuilder content = new StringBuilder();
        boolean comment = false;

        BufferedReader reader = new BufferedReader(new FileReader(filePath));
        String s;
        while ((s = reader.readLine()) != null) {
            if (s.length() > 0) {
                content.append(s.replaceAll(p1, " Integer ").replaceAll(p, "$2 $1").replaceAll(p2, "public $2 $1"));

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
        FileWriter writer = new FileWriter(filePath);
        writer.write(content.toString());
        writer.flush();
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
            if(last==')'&&s.indexOf("if(")!=-1){

            }else{
                content.append(";");
            }
        }
    }
}
