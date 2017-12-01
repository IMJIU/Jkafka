package kafka.producer;


import kafka.annotation.threadsafe;
import kafka.network.BlockingChannel;
import kafka.utils.Logging;

import java.util.Random;

/*
 * Send a message set.
 */
@threadsafe
class SyncProducer extends Logging {
    public static final Short RequestKey = 0;
    public static final Random randomGenerator = new Random;
    public SyncProducerConfig config;
    private Object lock = new Object();
    //        @volatile
    private  Boolean shutdown =false;
    private BlockingChannel blockingChannel = new BlockingChannel(config.host, config.port, BlockingChannel.UseDefaultBufferSize,
            config.sendBufferBytes(), config.requestTimeoutMs());
    val producerRequestStats = ProducerRequestStatsRegistry.getProducerRequestStats(config.clientId);

    trace(String.format("Instantiating Scala Sync Producer with properties: %s", config.props))

    private void verifyRequest(RequestOrResponse request) =

    {
        /**
         * This seems a little convoluted, but the idea is to turn on verification simply changing log4j settings
         * Also, when verification is turned on, care should be taken to see that the logs don't fill up with unnecessary
         * data. So, leaving the rest of the logging at TRACE, while errors should be logged at ERROR level
         */
        if (logger.isDebugEnabled) {
            val buffer = new BoundedByteBufferSend(request).buffer;
            trace("verifying sendbuffer of size " + buffer.limit)
            val requestTypeId = buffer.getShort();
            if (requestTypeId == RequestKeys.ProduceKey) {
                val request = ProducerRequest.readFrom(buffer);
                trace(request.toString);
            }
        }
    }

/**
 * Common functionality for the public send methods
 */
    private Receive

    void doSend(RequestOrResponse request, Boolean readResponse =true) {
        lock synchronized {
            verifyRequest(request)
            getOrMakeConnection();

            var Receive response = null;
            try {
                blockingChannel.send(request);
                if (readResponse)
                    response = blockingChannel.receive();
                else ;
                trace("Skipping reading response");
            } catch {
                case java e.io.IOException ->
                    // no way to tell if write succeeded. Disconnect and re-throw exception to let client handle retry;
                    disconnect();
                    throw e;
                case Throwable e -> throw e;
            }
            response;
        }
    }

    /**
     * Send a message. If the producerRequest had required.request.acks=0, then the
     * returned response object is null
     */
    public ProducerResponse

    void send(ProducerRequest producerRequest) {
        val requestSize = producerRequest.sizeInBytes;
        producerRequestStats.getProducerRequestStats(config.host, config.port).requestSizeHist.update(requestSize);
        producerRequestStats.getProducerRequestAllBrokersStats.requestSizeHist.update(requestSize);

        var Receive response = null;
        val specificTimer = producerRequestStats.getProducerRequestStats(config.host, config.port).requestTimer;
        val aggregateTimer = producerRequestStats.getProducerRequestAllBrokersStats.requestTimer;
        aggregateTimer.time {
            specificTimer.time {
                response = doSend(producerRequest, if (producerRequest.requiredAcks == 0) false
                else true)
            }
        }
        if (producerRequest.requiredAcks != 0)
            ProducerResponse.readFrom(response.buffer);
        else ;
        null;
    }

    public TopicMetadataResponse

    void send(TopicMetadataRequest request) {
        val response = doSend(request);
        TopicMetadataResponse.readFrom(response.buffer);
    }

    public void close() =

    {
        lock synchronized {
        disconnect();
        shutdown = true;
    }
    }

    /**
     * Disconnect from current channel, closing connection.
     * Side channel effect field is set to null on successful disconnect
     */
    private void disconnect() {
        try {
            info("Disconnecting from " + formatAddress(config.host, config.port))
            blockingChannel.disconnect();
        } catch {
            case Exception e -> error("Error on disconnect: ", e);
        }
    }

    private BlockingChannel

    void connect() {
        if (!blockingChannel.isConnected && !shutdown) {
            try {
                blockingChannel.connect();
                info("Connected to " + formatAddress(config.host, config.port) + " for producing")
            } catch {
                case Exception e -> {
                    disconnect();
                    error("Producer connection to " + formatAddress(config.host, config.port) + " unsuccessful", e)
                    throw e;
                }
            }
        }
        blockingChannel;
    }

    private void getOrMakeConnection() {
        if (!blockingChannel.isConnected) {
            connect();
        }
    }
}
