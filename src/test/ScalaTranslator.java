

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
public class ScalaTranslator {
    public final String env = "";
    public  final String main = "e:/github/JKafka/src/main/java/kafka/";
    public  final String test = "e:/github/JKafka/src/test/kafka/";
    static{

    }

    public static void main(String[] args) throws IOException {

//        System.out.println("ksdjfkasdf<Int> threadId)".replaceAll("([\\s\\(<])Int([\\s>])", "$1Integer$2"));
        List<String> filePaths = Arrays.asList(
//                main + "server/ReplicaManager.java",
//                main + "cluster/Partition.java",
//                main + "utils/Pool.java",
//                main + "log/LogConfig.java",
                main + "api/ApiUtils.java");
//        List<String> filePaths = Arrays.asList(main + "log/Log.java");
        for (String p : filePaths) {
            convertToJava(p, true);
        }
    }


    public static void convertToJava(String filePath, boolean write) throws IOException {
        char[] lastList = new char[]{'[', '(', '}', '{', ';', '*', '/', ',', '>', '=','+'};
        String[] ps = new String[]{
                " def ", "public void ",
                "<Int,", "<Integer,",
                 ",Int>", ",Integer>",
                "([\\s\\(<])Int([\\s>])", "$1Integer$2",
                "([\\s\\(<])Int([\\s>])", "$1Integer$2",
                "(\\w+):\\s?(\\w+)", "$2 $1",
                "public(.+):\\s?(\\w+)\\s?=", "public $2 $1",
                "(\".+\")\\.format\\(", "String.format($1,",
                "for\\((\\w+)\\s?\\<-\\s?(\\d+)\\s+until\\s+(\\d+)\\)", "for(int $1 = $2; $1 < $3; $1++)",
                "\\[(\\S+\\s?\\S+)\\]", "<$1>",
                "Int.MaxValue", "Integer.MAX_VALUE",
                "Long.MaxValue", "Long.MAX_VALUE",
                "Double.MaxValue", "Double.MAX_VALUE",
                " assertEquals", "Assert.assertEquals",
                " assertTrue"," Assert.assertTrue"
        };
        String p = "(\\w+):\\s?(\\w+)";
        StringBuilder content = new StringBuilder();
        boolean comment = false;
        BufferedReader reader = new BufferedReader(new FileReader(filePath));
        String s;
        while ((s = reader.readLine()) != null) {
            if (s.length() > 0) {
                for (int i = 0; i < ps.length; i = i + 2) {
                    s = s.replaceAll(ps[i], ps[i + 1]);
                }
                content.append(s);
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

    private static void addDelimit(char[] lastList, StringBuilder content, String s) {
        char last = s.charAt(s.length() - 1);
        boolean inLast = false;
        for (char c : lastList) {
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
