package kafka.cache.test;

import java.io.*;
import java.net.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author zhoulf
 * @create 2017-10-31 14:14
 **/
public class Client {
    public static void main(String[] args) throws IOException, InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(100);
        int taskSize = 100;
        CountDownLatch latch = new CountDownLatch(taskSize);
        long begin = System.currentTimeMillis();
        for (int i = 0; i < taskSize; i++) {
            executorService.submit(() -> {
                Socket socket = null;
                try {
                    socket = new Socket("localhost", 10086);
                    OutputStream os = socket.getOutputStream();//字节输出流
                    PrintWriter pw = new PrintWriter(os);//将输出流包装成打印流
                    pw.write("用户名：admin；密码：123");
                    pw.flush();
                    socket.shutdownOutput();
                    InputStream is = socket.getInputStream();
//                    BufferedReader br = new BufferedReader(new InputStreamReader(is));
                    String info;
                    byte[] bs = new byte[1024];
                    int len = 0;
                    while ((len = is.read(bs)) != -1) {
                        System.out.println(new String(bs, 0, len));
                    }
//                    br.close();
                    is.close();
                    pw.close();
                    os.close();
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();
        System.out.print(System.currentTimeMillis() - begin);
        System.out.println("ms");
        executorService.shutdown();
    }
}
