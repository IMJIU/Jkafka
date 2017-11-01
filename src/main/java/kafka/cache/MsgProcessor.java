package kafka.cache;


import com.google.common.collect.Lists;
import kafka.cache.file.FileBufferPool;
import kafka.cache.mem.LackMemException;
import kafka.cache.mem.SendBuffer;
import kafka.cache.mem.SocketSendBufferPool;
import kafka.network.RequestChannel;
import kafka.network.SocketServer;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * @author zhoulf
 * @create 2017-10-31 14:28
 **/
public class MsgProcessor {
    public FileBufferPool filePool;
    public SocketSendBufferPool memPool;
    public int coreSize = Runtime.getRuntime().availableProcessors();
    public ArrayBlockingQueue<MsgBufferSend>[] queues = new ArrayBlockingQueue[coreSize];
    public ExecutorService processorExecutor = Executors.newFixedThreadPool(coreSize);
    public SocketServer server;

    /**
     * Choose a number of random available ports
     */
    public static List<Integer> choosePorts(Integer count) throws IOException {
        List<ServerSocket> socketList = Lists.newArrayList();
        for (int i = 0; i < count; i++) {
            socketList.add(new ServerSocket(0));
        }
        List<Integer> ports = socketList.stream().map(s -> s.getLocalPort()).collect(Collectors.toList());
        socketList.forEach(s -> {
            try {
                s.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        return ports;
    }


    /**
     * Choose an available port
     */
    public static Integer choosePort() throws IOException {
        return choosePorts(1).get(0);
    }

    public MsgProcessor() {
        try {
            server = new SocketServer(-1,null, choosePort(),
                    coreSize,
                    1000,
                    300000,
                    300000,
                    1024*1024,
                    100,
                    60 * 1000L,
                    Collections.emptyMap());
            server.startup();
            filePool = new FileBufferPool();
            memPool = new SocketSendBufferPool();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        initProcessor();
    }

    public void send(Socket sockets, byte[] msg) throws IOException {
        SendBuffer sendBuffer = memPool.acquire(msg);

    }

    private void initProcessor() {
        for (int i = 0; i < coreSize; i++) {
            processorExecutor.submit(new Worker(server.requestChannel, filePool, memPool));
        }
    }

    static class Worker implements Runnable {
        private RequestChannel requestChannel;
        FileBufferPool filePool;
        SocketSendBufferPool memPool;

        public Worker(RequestChannel requestChannel, FileBufferPool filePool, SocketSendBufferPool memPool) {
            this.requestChannel = requestChannel;
            this.filePool = filePool;
            this.memPool = memPool;
        }

        @Override
        public void run() {
            while (true) {
                RequestChannel.Request request = requestChannel.receiveRequest();
                ByteBuffer result = handlerInovker(request);
                MsgBufferSend msgBufferSend = null;
                try {
                    msgBufferSend = new MsgBufferSend("1", memPool.acquire(result.array()));
                } catch (LackMemException e) {
                    System.err.println("mem lack! write disk");
                    try {
                        msgBufferSend = new MsgBufferSend("1", filePool.append(result));
                    } catch (IOException e1) {
                        e1.printStackTrace();
                    }
                }catch (Exception e2){
                    e2.printStackTrace();
                }
                requestChannel.sendResponse(new RequestChannel.Response(request, msgBufferSend));
            }
        }

        private ByteBuffer handlerInovker(RequestChannel.Request request) {
//            return ByteBuffer.wrap("hello i like you".getBytes());
            return ByteBuffer.wrap(App.bytes);
        }
    }
}


