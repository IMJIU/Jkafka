package kafka.network;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import kafka.api.ProducerRequest;
import kafka.producer.SyncProducerConfig;
import kafka.utils.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * @author
 * @create 2017-04-28 02 11
 **/
public class SocketServerTest {
    SocketServer server;

    @Before
    public void setup() throws IOException, InterruptedException {
        server = new SocketServer(0,null, kafka.utils.TestUtils.choosePort(),
                1,
                50,
                300000,
                300000,
                50,
                5,
                60 * 1000L,
                Collections.emptyMap());
        server.startup();
    }

    public void sendRequest(Socket socket, Short id, byte[] request) throws IOException {
        DataOutputStream outgoing = new DataOutputStream(socket.getOutputStream());
        outgoing.writeInt(request.length + 2);
        outgoing.writeShort(id);
        outgoing.write(request);
        outgoing.flush();
    }

    public byte[] receiveResponse(Socket socket) throws IOException {
        DataInputStream incoming = new DataInputStream(socket.getInputStream());
        Integer len = incoming.readInt();
        byte[] response = new byte[len];
        incoming.readFully(response);
        return response;
    }

    /* A simple request handler that just echos back the response */
    public void processRequest(RequestChannel channel) {
        RequestChannel.Request request = channel.receiveRequest();
        ByteBuffer byteBuffer = ByteBuffer.allocate(request.requestObj.sizeInBytes());
        request.requestObj.writeTo(byteBuffer);
        byteBuffer.rewind();
        BoundedByteBufferSend send = new BoundedByteBufferSend(byteBuffer);
        channel.sendResponse(new RequestChannel.Response(request.processor, request, send));
    }

    public Socket connect() throws IOException {
        return connect(server);
    }

    public Socket connect(SocketServer s) throws IOException {
        return new Socket("localhost", s.port);
    }

    @After
    public void cleanup() throws InterruptedException {
        server.shutdown();
    }

    /**
     * 模拟客户端发送request请求  再模拟发送response响应（只是把request的字节发送）
     * 再对比发送的结果 与 接收到的结果
     */
    @Test
    public void simpleRequest() throws IOException, InterruptedException {
        Socket socket = connect();
        Integer correlationId = -1;
        String clientId = SyncProducerConfig.DefaultClientId;
        Integer ackTimeoutMs = SyncProducerConfig.DefaultAckTimeoutMs;
        Short ack = SyncProducerConfig.DefaultRequiredAcks;
        ProducerRequest emptyRequest = new ProducerRequest(correlationId, clientId, ack, ackTimeoutMs, Maps.newHashMap());

        ByteBuffer byteBuffer = ByteBuffer.allocate(emptyRequest.sizeInBytes());
        emptyRequest.writeTo(byteBuffer);
        byteBuffer.rewind();
        byte[] serializedBytes = new byte[byteBuffer.remaining()];
        byteBuffer.get(serializedBytes);

        sendRequest(socket, (short) 0, serializedBytes);//size|clientId|request
        processRequest(server.requestChannel);
        TestUtils.checkEquals(ByteBuffer.wrap(serializedBytes), ByteBuffer.wrap(receiveResponse(socket)));
    }

    /**
     * 发送超过配置的最大消息大小（maxRequestSize），报错
     */
    @Test(expected = IOException.class)
    public void tooBigRequestIsRejected() throws IOException {
        byte[] tooManyBytes = new byte[server.maxRequestSize + 1];
        new Random().nextBytes(tooManyBytes);
        Socket socket = connect();
        sendRequest(socket, (short) 0, tooManyBytes);
        receiveResponse(socket);
    }

    /**
     * 发送request完后(读取完request, attach到key)，interestOps应该不为op_read
     * 发送response后(send写入channel)，interestOps应该为op_read
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testNullResponse() throws IOException, InterruptedException {
        Socket socket = connect();
        byte[] bytes = new byte[40];
        sendRequest(socket, (short) 0, bytes);

        RequestChannel.Request request = server.requestChannel.receiveRequest();
        // Since the response is not sent yet, the selection key should not be readable.;
        TestUtils.waitUntilTrue(() -> (((SelectionKey) request.requestKey).interestOps() & SelectionKey.OP_READ) != SelectionKey.OP_READ,
                "Socket key shouldn't be available for read");

        server.requestChannel.sendResponse(new RequestChannel.Response(0, request, null));

        // After the response is sent to the client (which is async and may take a bit of time), the socket key should be available for reads.;
        TestUtils.waitUntilTrue(() -> (((SelectionKey) request.requestKey).interestOps() & SelectionKey.OP_READ) == SelectionKey.OP_READ,
                "Socket key should be available for read");
    }

    /**
     * shutdown后 发送请求报错
     */
    @Test(expected = IOException.class)
    public void testSocketsCloseOnShutdown() throws IOException, InterruptedException {
        // open a connection;
        Socket socket = connect();
        byte[] bytes = new byte[40];
        // send a request first to make sure the connection has been picked up by the socket server;
        sendRequest(socket, (short) 0, bytes);
        processRequest(server.requestChannel);
        // then shutdown the server;
        server.shutdown();
        // doing a subsequent send should throw an exception as the connection should be closed.;
        sendRequest(socket, (short) 0, bytes);
    }

    /**
     * 达到配置每个IP最大连接后，再连接读取为-1(因为accepter会有TooManyConnectionsException，把socket关掉了)
     */
    @Test
    public void testMaxConnectionsPerIp() throws IOException {
        // make the maximum allowable number of connections and then leak them;
        List<Socket> conns = Stream.iterate(0, n -> n + 1).limit(server.maxConnectionsPerIp).map(i -> {
            try {
                return connect();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }).collect(Collectors.toList());
        // now try one more (should fail);
        Socket conn = connect();
        conn.setSoTimeout(3000);
        Assert.assertEquals(-1, conn.getInputStream().read());
    }

    /**
     * 跟maxConnectionsPerIp差不多，只是重写这个参数值
     * 达到配置每个IP最大连接后，再连接读取为-1(因为accepter会有TooManyConnectionsException，把socket关掉了)
     */
    @Test
    public void testMaxConnectionsPerIPOverrides() throws IOException, InterruptedException {
        Integer overrideNum = 6;
        Map<String, Integer> overrides = ImmutableMap.of("localhost", overrideNum);
        SocketServer overrideServer = new SocketServer(0,
                null,
                kafka.utils.TestUtils.choosePort(),
                1,
                50,
                300000,
                300000,
                50,
                5,
                60 * 1000L,
                overrides);
        overrideServer.startup();
        // make the maximum allowable number of connections and then leak them;

        List<Socket> conns = Stream.iterate(0, n -> n + 1).limit(overrideNum).map(i -> {
            try {
                return connect(overrideServer);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }).collect(Collectors.toList());
        // now try one more (should fail);
        Socket conn = connect(overrideServer);
        conn.setSoTimeout(3000);
        Assert.assertEquals(-1, conn.getInputStream().read());
        overrideServer.shutdown();
    }

}
