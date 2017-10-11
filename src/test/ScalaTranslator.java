
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * @author
 * @create 2017-03-30 10:04
 **/
public class ScalaTranslator {


    public static void main(String[] args) throws IOException {
//        System.out.println("ksdjfkasdf<Int> threadId)".replaceAll("([\\s\\(<])Int([\\s>])", "$1Integer$2"));
        List<String> filePaths = Arrays.asList(
//                main + "server/ReplicaManager.java",
//                main + "cluster/Partition.java",
//                main + "utils/Pool.java",
//                main + "log/LogConfig.java",
             //   main + "server/OffsetManagerConfig.java"
                  test + "api/SerializationTestUtils.java"
                );
//        List<String> filePaths = Arrays.asList(main + "log/Log.java");
        for (String p : filePaths) {
            convertToJava(p, true);
        }
    }


    public static void convertToJava(String filePath, boolean write) throws IOException {
        List<Character> lastList = Arrays.asList('[', '(', '}', '{', ';', '*', '/', ',', '>', '=', '+');
        String[] ps = new String[]{
                " Int ", " Integer ",
                "\\[(\\D+)\\]","<$1>",
                "\\sInt\\s", " Integer ",
                "Int>", "Integer>",
                "<Int,", "<Integer,>",
                " def ", "public void ",
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
                "val\\s(\\S+)\\s?\\s?:\\s?(\\S+)", "$2 $1",
                " assertTrue", " Assert.assertTrue"
        };
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

    @SuppressWarnings("rawtypes")
    public static String getMyIp() {
        String localip = null;// 本地IP，如果没有配置外网IP则返回它
        String netip = null;// 外网IP
        try {
            Enumeration netInterfaces = NetworkInterface.getNetworkInterfaces();
            InetAddress ip = null;
            boolean finded = false;// 是否找到外网IP
            while (netInterfaces.hasMoreElements() && !finded) {
                NetworkInterface ni = (NetworkInterface) netInterfaces.nextElement();
                Enumeration address = ni.getInetAddresses();
                while (address.hasMoreElements()) {
                    ip = (InetAddress) address.nextElement();
                    if (!ip.isSiteLocalAddress() && !ip.isLoopbackAddress() && ip.getHostAddress().indexOf(":") ==
                            -1) {// 外网IP
                        netip = ip.getHostAddress();
                        finded = true;
                        break;
                    } else if (ip.isSiteLocalAddress() && !ip.isLoopbackAddress() && ip.getHostAddress().indexOf(":")
                            == -1) {// 内网IP
                        localip = ip.getHostAddress();
                    }
                }
            }
        } catch (SocketException e) {
            e.printStackTrace();
        }

        if (netip != null && !"".equals(netip)) {
            return netip;
        } else {
            return localip;
        }
    }
    public static String main = "g:/github/JKafka/src/main/java/kafka/";
    public static String test = "g:/github/JKafka/src/test/java/kafka/";

    static {
        String ip = getMyIp();
        if (ip.equals("10.8.72.109")) {
            main = "e:/github/JKafka/src/main/java/kafka/";
            test = "e:/github/JKafka/src/test/kafka/";
        }
    }
}

