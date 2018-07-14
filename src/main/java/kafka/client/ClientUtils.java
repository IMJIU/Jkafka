package kafka.client;

/**
 * @author zhoulf
 * @create 2017-12-06 18 9
 **/

import kafka.api.ConsumerMetadataRequest;
import kafka.api.ConsumerMetadataResponse;
import kafka.api.TopicMetadataRequest;
import kafka.api.TopicMetadataResponse;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.KafkaException;
import kafka.network.BlockingChannel;
import kafka.network.Receive;
import kafka.producer.ProducerConfig;
import kafka.producer.ProducerPool;
import kafka.producer.SyncProducer;
import kafka.utils.Logging;
import kafka.utils.Sc;
import kafka.utils.Utils;
import org.I0Itec.zkclient.ZkClient;

import static org.apache.kafka.common.utils.Utils.*;
import static kafka.utils.ZkUtils.*;

import java.io.IOException;
import java.util.*;

/**
 * Helper functions common to clients (producer, consumer, or admin)
 */
public class ClientUtils {
    private static final Logging log = Logging.getLogger(ClientUtils.class.getName());

    /**
     * Used by the producer to send a metadata request since it has access to the ProducerConfig
     *
     * @param topics         The topics for which the metadata needs to be fetched
     * @param brokers        The brokers in the cluster as configured on the producer through metadata.broker.list
     * @param producerConfig The producer's config
     * @return topic metadata response
     */
    public static TopicMetadataResponse fetchTopicMetadata(Set<String> topics, List<Broker> brokers, ProducerConfig producerConfig, Integer correlationId) {
        Boolean fetchMetaDataSucceeded = false;
        Integer i = 0;
        TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(TopicMetadataRequest.CurrentVersion, correlationId, producerConfig.clientId(), Sc.toList(topics));
        TopicMetadataResponse topicMetadataResponse = null;
        Throwable t = null;
        // shuffle the list of brokers before sending metadata requests so that most requests don't get routed to the;
        // same broker;
        Collections.shuffle(brokers);
        List<Broker> shuffledBrokers = brokers;
        while (i < shuffledBrokers.size() && !fetchMetaDataSucceeded) {
            SyncProducer producer = ProducerPool.createSyncProducer(producerConfig, shuffledBrokers.get(i));
            log.info(String.format("Fetching metadata from broker %s with correlation id %d for %d topic(s) %s", shuffledBrokers.get(i), correlationId, topics.size(), topics));
            try {
                topicMetadataResponse = producer.send(topicMetadataRequest);
                fetchMetaDataSucceeded = true;
            } catch (Throwable e) {
                log.warn(String.format("Fetching topic metadata with correlation id %d for topics <%s] from broker [%s> failed",
                        correlationId, topics, shuffledBrokers.get(i).toString()), e);
                t = e;
            } finally {
                i = i + 1;
                producer.close();
            }
        }
        if (!fetchMetaDataSucceeded) {
            throw new KafkaException(String.format("fetching topic metadata for topics <%s] from broker [%s> failed", topics, shuffledBrokers), t);
        } else {
            log.debug(String.format("Successfully fetched metadata for %d topic(s) %s", topics.size(), topics));
        }
        return topicMetadataResponse;
    }

    /**
     * Used by a non-producer client to send a metadata request
     *
     * @param topics   The topics for which the metadata needs to be fetched
     * @param brokers  The brokers in the cluster as configured on the client
     * @param clientId The client's identifier
     * @return topic metadata response
     */
    public static TopicMetadataResponse fetchTopicMetadata(Set<String> topics, List<Broker> brokers, String clientId, Integer timeoutMs,
                                                           Integer correlationId) {
        if (correlationId == null) {
            correlationId = 0;
        }
        Properties props = new Properties();
        props.put("metadata.broker.list", Sc.mkString(Sc.map(brokers, b -> b.connectionString()), ","));
        props.put("client.id", clientId);
        props.put("request.timeout.ms", timeoutMs.toString());
        ProducerConfig producerConfig = new ProducerConfig(props);
        return fetchTopicMetadata(topics, brokers, producerConfig, correlationId);
    }

    /**
     * Parse a list of broker urls in the form port1 host1, port2 host2, ...
     */
    public static List<Broker> parseBrokerList(String brokerListStr) {
        List<String> brokersStr = Utils.parseCsvList(brokerListStr);
        return Sc.map(Sc.zipWithIndex(brokersStr),
                (address, brokerId) -> new Broker(brokerId, getHost(address), getPort(address)));
    }

    /**
     * Creates a blocking channel to a random broker
     */
    public static BlockingChannel channelToObjectBroker(ZkClient zkClient, Integer socketTimeoutMs) {
        if (socketTimeoutMs == null) socketTimeoutMs = 3000;
        final Integer socketTimeoutMs_f = socketTimeoutMs;
        BlockingChannel channel = null;
        boolean connected = false;
        while (!connected) {
            List<Broker> allBrokers = getAllBrokersInCluster(zkClient);
            Collections.shuffle(allBrokers);
            boolean find = false;
            for (Broker broker : allBrokers) {
                if (!find) {
                    log.trace(String.format("Connecting to broker %s:%d.", broker.host, broker.port));
                    try {
                        channel = new BlockingChannel(broker.host, broker.port, BlockingChannel.UseDefaultBufferSize, BlockingChannel.UseDefaultBufferSize, socketTimeoutMs_f);
                        channel.connect();
                        log.debug(String.format("Created channel to broker %s:%d.", channel.host, channel.port));
                        find = true;
                    } catch (Throwable e) {
                        if (channel != null) channel.disconnect();
                        channel = null;
                        log.info(String.format("Error while creating channel to %s:%d.", broker.host, broker.port));
                        find = false;
                    }
                }
            }
            connected = (channel == null) ? false : true;
        }

        return channel;
    }

    /**
     * Creates a blocking channel to the offset manager of the given group
     */
    public static BlockingChannel channelToOffsetManager(String group, ZkClient zkClient, Integer socketTimeoutMs, Integer retryBackOffMs) {
        if (socketTimeoutMs == null) socketTimeoutMs = 300;
        if (retryBackOffMs == null) retryBackOffMs = 1000;
        BlockingChannel queryChannel = channelToObjectBroker(zkClient, 3000);

        Optional<BlockingChannel> offsetManagerChannelOpt = Optional.empty();

        while (!offsetManagerChannelOpt.isPresent()) {
            Optional<Broker> coordinatorOpt = Optional.empty();
            while (!coordinatorOpt.isPresent()) {
                try {
                    if (!queryChannel.isConnected())
                        queryChannel = channelToObjectBroker(zkClient, 3000);
                    log.debug(String.format("Querying %s:%d to locate offset manager for %s.", queryChannel.host, queryChannel.port, group));
                    queryChannel.send(new ConsumerMetadataRequest(group));
                    Receive response = queryChannel.receive();
                    ConsumerMetadataResponse consumerMetadataResponse = ConsumerMetadataResponse.readFrom(response.buffer());
                    log.debug("Consumer metadata response: " + consumerMetadataResponse.toString());
                    if (consumerMetadataResponse.errorCode == ErrorMapping.NoError)
                        coordinatorOpt = consumerMetadataResponse.coordinatorOpt;
                    else {
                        log.debug(String.format("Query to %s:%d to locate offset manager for %s failed - will retry in %d milliseconds.",
                                queryChannel.host, queryChannel.port, group, retryBackOffMs));
                        Thread.sleep(retryBackOffMs);
                    }
                } catch (IOException ioe) {
                    log.info(String.format("Failed to fetch consumer metadata from %s:%d.", queryChannel.host, queryChannel.port));
                    queryChannel.disconnect();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            Broker coordinator = coordinatorOpt.get();
            if (coordinator.host == queryChannel.host && coordinator.port == queryChannel.port) {
                offsetManagerChannelOpt = Optional.of(queryChannel);
            } else {
                String connectString = String.format("%s:%d", coordinator.host, coordinator.port);
                BlockingChannel offsetManagerChannel = null;
                try {
                    log.debug(String.format("Connecting to offset manager %s.", connectString));
                    offsetManagerChannel = new BlockingChannel(coordinator.host, coordinator.port,
                            BlockingChannel.UseDefaultBufferSize,
                            BlockingChannel.UseDefaultBufferSize,
                            socketTimeoutMs);
                    offsetManagerChannel.connect();
                    offsetManagerChannelOpt = Optional.of(offsetManagerChannel);
                    queryChannel.disconnect();
                } catch (Throwable ioe) { // offsets manager may have moved;
                    log.info(String.format("Error while connecting to %s.", connectString));
                    if (offsetManagerChannel != null) offsetManagerChannel.disconnect();
                    try {
                        Thread.sleep(retryBackOffMs);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    offsetManagerChannelOpt = Optional.empty(); // just in case someone decides to change shutdownChannel to not swallow exceptions;
                    throw new RuntimeException(ioe);
                }
            }
        }

        return offsetManagerChannelOpt.get();
    }
}
