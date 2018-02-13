package kafka.producer;


import kafka.annotation.threadsafe;
import kafka.api.*;
import kafka.common.FinalObject;
import kafka.metrics.KafkaTimer;
import kafka.network.BlockingChannel;
import kafka.network.BoundedByteBufferSend;
import kafka.network.Receive;
import kafka.utils.Logging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import static org.apache.kafka.common.utils.Utils.*;

/*
 * Send a message set.
 */
@threadsafe
public class SyncProducer extends Logging {
    public static final Short RequestKey = 0;
    public static final Random randomGenerator = new Random();
    public SyncProducerConfig config;
    private Object lock = new Object();
    //        @volatile
    private Boolean shutdown = false;
    private BlockingChannel blockingChannel;
    ProducerRequestStats producerRequestStats;

    public SyncProducer(SyncProducerConfig config) {
        this.config = config;
        blockingChannel = new BlockingChannel(config.host, config.port, BlockingChannel.UseDefaultBufferSize,
                config.sendBufferBytes(), config.requestTimeoutMs());
        producerRequestStats = ProducerRequestStatsRegistry.getProducerRequestStats(config.clientId());
        trace(String.format("Instantiating Scala Sync Producer with properties: %s", config.props));
    }

    private void verifyRequest(RequestOrResponse request) {
        /**
         * This seems a little convoluted, but the idea is to turn on verification simply changing log4j settings
         * Also, when verification is turned on, care should be taken to see that the logs don't fill up with unnecessary
         * data. So, leaving the rest of the logging at TRACE, while errors should be logged at ERROR level
         */
        if (logger.isDebugEnabled()) {
            ByteBuffer buffer = new BoundedByteBufferSend(request).buffer;
            trace("verifying sendbuffer of size " + buffer.limit());
            short requestTypeId = buffer.getShort();
            if (requestTypeId == RequestKeys.ProduceKey) {
                ProducerRequest request2 = ProducerRequest.readFrom.handle(buffer);
                trace(request2.toString());
            }
        }
    }

    /**
     * Common functionality for the public send methods
     */
    private Receive doSend(RequestOrResponse request) throws IOException {
        return doSend(request, true);
    }

    private Receive doSend(RequestOrResponse request, Boolean readResponse) throws IOException {
        synchronized (lock) {
            verifyRequest(request);
            getOrMakeConnection();

            Receive response = null;
            try {
                blockingChannel.send(request);
                if (readResponse)
                    response = blockingChannel.receive();
                else
                    trace("Skipping reading response");
            } catch (IOException e) {
                // no way to tell if write succeeded. Disconnect and re-throw exception to let client handle retry;
                disconnect();
                throw new IOException(e);
            } catch (Throwable e) {
                throw e;
            }
            return response;
        }
    }

    /**
     * Send a message. If the producerRequest had required.request.acks=0, then the
     * returned response object is null
     */
    public ProducerResponse send(ProducerRequest producerRequest) {
        int requestSize = producerRequest.sizeInBytes();
        producerRequestStats.getProducerRequestStats(config.host, config.port).requestSizeHist.update(requestSize);
        producerRequestStats.getProducerRequestAllBrokersStats().requestSizeHist.update(requestSize);

        Receive response;
        FinalObject<Receive> finalObject = FinalObject.of();
        KafkaTimer specificTimer = producerRequestStats.getProducerRequestStats(config.host, config.port).requestTimer;
        KafkaTimer aggregateTimer = producerRequestStats.getProducerRequestAllBrokersStats().requestTimer;
        aggregateTimer.time(() ->
                specificTimer.time(() -> {
                    try {
                        finalObject.set(doSend(producerRequest, (producerRequest.requiredAcks == 0) ? false : true));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    return finalObject;
                })
        );
        response = finalObject.get();
        if (producerRequest.requiredAcks != 0)
            return ProducerResponse.readFrom.handle(response.buffer());
        else
            return null;
    }

    public TopicMetadataResponse send(TopicMetadataRequest request) throws IOException {
        Receive response = doSend(request);
        return TopicMetadataResponse.readFrom(response.buffer());
    }

    public void close() {
        synchronized (lock) {
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
            info("Disconnecting from " + formatAddress(config.host, config.port));
            blockingChannel.disconnect();
        } catch (Exception e) {
            error("Error on disconnect: ", e);
        }
    }

    private BlockingChannel connect() {
        if (!blockingChannel.isConnected() && !shutdown) {
            try {
                blockingChannel.connect();
                info("Connected to " + formatAddress(config.host, config.port) + " for producing");
            } catch (Exception e) {
                disconnect();
                error("Producer connection to " + formatAddress(config.host, config.port) + " unsuccessful", e);
                throw e;
            }
        }
        return blockingChannel;
    }

    private void getOrMakeConnection() {
        if (!blockingChannel.isConnected()) {
            connect();
        }
    }
}
