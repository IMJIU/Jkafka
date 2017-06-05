package kafka.tool;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Properties;

/**
 * Created by Administrator on 2017/6/1.
 */
public abstract class MessageFormatter {
    public abstract  void writeTo(byte[] key, byte[] value, PrintStream output) throws IOException;

    public void init(Properties props){};

    public  void close(){};
}
