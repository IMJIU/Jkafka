package kafka.cache;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author zhoulf
 * @create 2017-10-31 11:22
 **/
public class App {
    public static final int coreSize = Runtime.getRuntime().availableProcessors();
    public static final ExecutorService socketExecutor = Executors.newCachedThreadPool();
    public static byte[] bytes = null;

    public static void main(String[] args) throws IOException {
        Level level = Level.toLevel(Level.TRACE_INT);
        LogManager.getRootLogger().setLevel(level);
        bytes = Files.readAllBytes(Paths.get("d:\\access.log"));
        MsgProcessor processor = new MsgProcessor();
    }

    public static void socket() {
        /**
         * 基于TCP协议的Socket通信，实现用户登录，服务端
         */
        ServerSocket serverSocket = null;//1024-65535的某个端口
        try {
            serverSocket = new ServerSocket(10086);
            while (true) {
                Socket socket = serverSocket.accept();
                accept(socket);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                serverSocket.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

    private static void accept(Socket socket) {
        socketExecutor.submit(() -> {
            BufferedReader br = null;
            OutputStream out = null;
            SocketChannel channel;
            try {
                channel = socket.getChannel();
                br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String info;
                while ((info = br.readLine()) != null) {
                    channel.write(ByteBuffer.wrap(bytes));
                    Thread.sleep(10L);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                try {
                    socket.shutdownInput();
                    socket.close();
                    out.close();
                    br.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }


}
