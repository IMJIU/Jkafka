package kafka.network;/**
 * Created by zhoulf on 2017/4/28.
 */

/**
 * @author
 * @create 2017-04-28 02 11
 **/
public class SocketServerTest {

         SocketServer server = new SocketServer(0,
                                                    host = null,
                                                    port = kafka.utils.TestUtils.choosePort,
                                                    numProcessorThreads = 1,
                                                    maxQueuedRequests = 50,
                                                    sendBufferSize = 300000,
                                                    recvBufferSize = 300000,
                                                    maxRequestSize = 50,
                                                    maxConnectionsPerIp = 5,
                                                    connectionsMaxIdleMs = 60*1000,
                                                    maxConnectionsPerIpOverrides = Map.empty<String,Int>);
  server.startup();

       public void sendRequest(Socket socket, Short id, Array request<Byte>) {
            val outgoing = new DataOutputStream(socket.getOutputStream);
            outgoing.writeInt(request.length + 2);
            outgoing.writeShort(id);
            outgoing.write(request);
            outgoing.flush();
        }

       public void receiveResponse(Socket socket): Array<Byte> = {
            val incoming = new DataInputStream(socket.getInputStream);
            val len = incoming.readInt();
            val response = new Array<Byte>(len);
                    incoming.readFully(response);
            response;
        }

        /* A simple request handler that just echos back the response */
       public void processRequest(RequestChannel channel) {
            val request = channel.receiveRequest;
            val byteBuffer = ByteBuffer.allocate(request.requestObj.sizeInBytes);
            request.requestObj.writeTo(byteBuffer);
            byteBuffer.rewind();
            val send = new BoundedByteBufferSend(byteBuffer);
            channel.sendResponse(new RequestChannel.Response(request.processor, request, send));
        }

       public void connect(SocketServer s = server) = new Socket("localhost", s.port);

        @After
       public void cleanup() {
            server.shutdown();
        }
        @Test
       public void simpleRequest() {
            val socket = connect();
            val correlationId = -1;
            val clientId = SyncProducerConfig.DefaultClientId;
            val ackTimeoutMs = SyncProducerConfig.DefaultAckTimeoutMs;
            val ack = SyncProducerConfig.DefaultRequiredAcks;
            val emptyRequest =
                    new ProducerRequest(correlationId, clientId, ack, ackTimeoutMs, collection.mutable.Map<TopicAndPartition, ByteBufferMessageSet>());

            val byteBuffer = ByteBuffer.allocate(emptyRequest.sizeInBytes);
            emptyRequest.writeTo(byteBuffer);
            byteBuffer.rewind();
            val serializedBytes = new Array<Byte>(byteBuffer.remaining);
                    byteBuffer.get(serializedBytes);

            sendRequest(socket, 0, serializedBytes);
            processRequest(server.requestChannel);
           Assert.assertEquals(serializedBytes.toSeq, receiveResponse(socket).toSeq);
        }

        @Test(expected = classOf<IOException>)
       public void tooBigRequestIsRejected() {
            val tooManyBytes = new Array<Byte>(server.maxRequestSize + 1);
            new Random().nextBytes(tooManyBytes);
            val socket = connect();
            sendRequest(socket, 0, tooManyBytes);
            receiveResponse(socket);
        }

        @Test
       public void testNullResponse() {
            val socket = connect();
            val bytes = new Array<Byte>(40);
            sendRequest(socket, 0, bytes);

            val request = server.requestChannel.receiveRequest;
            // Since the response is not sent yet, the selection key should not be readable.;
            TestUtils.waitUntilTrue(
                    () => { (request.requestKey.asInstanceOf<SelectionKey>.interestOps & SelectionKey.OP_READ) != SelectionKey.OP_READ },
                    "Socket key shouldn't be available for read")

            server.requestChannel.sendResponse(new RequestChannel.Response(0, request, null));

            // After the response is sent to the client (which is async and may take a bit of time), the socket key should be available for reads.;
            TestUtils.waitUntilTrue(
                    () => { (request.requestKey.asInstanceOf<SelectionKey>.interestOps & SelectionKey.OP_READ) == SelectionKey.OP_READ },
                    "Socket key should be available for reads")
        }

        @Test(expected = classOf<IOException>)
       public void testSocketsCloseOnShutdown() {
            // open a connection;
            val socket = connect();
            val bytes = new Array<Byte>(40);
            // send a request first to make sure the connection has been picked up by the socket server;
            sendRequest(socket, 0, bytes);
            processRequest(server.requestChannel);
            // then shutdown the server;
            server.shutdown();
            // doing a subsequent send should throw an exception as the connection should be closed.;
            sendRequest(socket, 0, bytes);
        }

        @Test
       public void testMaxConnectionsPerIp() {
            // make the maximum allowable number of connections and then leak them;
            val conns = (0 until server.maxConnectionsPerIp).map(i => connect());
            // now try one more (should fail);
            val conn = connect();
            conn.setSoTimeout(3000);
           Assert.assertEquals(-1, conn.getInputStream().read());
        }
        @Test
       public Unit  void testMaxConnectionsPerIPOverrides() {
            val overrideNum = 6;
            val Map overrides<String, Int> = Map("localhost" -> overrideNum);
            val SocketServer overrideServer = new SocketServer(0,
                    host = null,
                    port = kafka.utils.TestUtils.choosePort,
                    numProcessorThreads = 1,
                    maxQueuedRequests = 50,
                    sendBufferSize = 300000,
                    recvBufferSize = 300000,
                    maxRequestSize = 50,
                    maxConnectionsPerIp = 5,
                    connectionsMaxIdleMs = 60*1000,
                    maxConnectionsPerIpOverrides = overrides);
            overrideServer.startup();
            // make the maximum allowable number of connections and then leak them;
            val conns = ((0 until overrideNum).map(i => connect(overrideServer)));
            // now try one more (should fail);
            val conn = connect(overrideServer);
            conn.setSoTimeout(3000);
           Assert.assertEquals(-1, conn.getInputStream.read());
            overrideServer.shutdown();
        }

}
