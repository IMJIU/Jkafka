package kafka.server;

/**
 * @author zhoulf
 * @create 2017-11-27 59 17
 **/

import com.yammer.metrics.core.Gauge;
import kafka.admin.AdminUtils;
import kafka.api.ControlledShutdownRequest;
import kafka.api.ControlledShutdownResponse;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.controller.KafkaController;

import kafka.log.CleanerConfig;
import kafka.log.LogConfig;
import kafka.log.LogManager;
import kafka.metrics.KafkaMetricsGroup;
import kafka.network.BlockingChannel;
import kafka.network.Receive;
import kafka.network.SocketServer;
import kafka.utils.*;
import org.I0Itec.zkclient.ZkClient;

import java.io.File;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static kafka.server.BrokerStates.*;

/**
 * Represents the lifecycle of a single Kafka broker. Handles all functionality required
 * to start up and shutdown a single Kafka node.
 */
class KafkaServer extends KafkaMetricsGroup {
    KafkaConfig config;
    Time time;

    public KafkaServer(KafkaConfig config, Time time) {
        this.config = config;
        this.time = time;
        this.logIdent = "<Kafka Server " + config.brokerId + ">, ";
        newGauge("BrokerState", new Gauge<Object>() {
            public Object value() {
                return brokerState.currentState;
            }
        });
    }

    private AtomicBoolean isShuttingDown = new AtomicBoolean(false);
    private CountDownLatch shutdownLatch = new CountDownLatch(1);
    private AtomicBoolean startupComplete = new AtomicBoolean(false);
    BrokerState brokerState = new BrokerState();
    AtomicInteger correlationId = new AtomicInteger(0);
    SocketServer socketServer = null;
    KafkaRequestHandlerPool requestHandlerPool = null;
    LogManager logManager = null;
    OffsetManager offsetManager = null;
    KafkaHealthcheck kafkaHealthcheck = null;
    TopicConfigManager topicConfigManager = null;
    ReplicaManager replicaManager = null;
    KafkaApis apis = null;
    KafkaController kafkaController = null;
    KafkaScheduler kafkaScheduler = new KafkaScheduler(config.backgroundThreads);
    ZkClient zkClient = null;


    /**
     * Start up API for bringing up a single instance of the Kafka server.
     * Instantiates the LogManager, the SocketServer and the request handlers - KafkaRequestHandlers
     */
    public void startup() {
        try {
            info("starting");
            brokerState.newState(Starting);
            isShuttingDown = new AtomicBoolean(false);
            shutdownLatch = new CountDownLatch(1);

      /* start scheduler */
            kafkaScheduler.startup();

      /* setup zookeeper */
            zkClient = initZk();

      /* start log manager */
            logManager = createLogManager(zkClient, brokerState);
            logManager.startup();

            socketServer = new SocketServer(config.brokerId,
                    config.hostName,
                    config.port,
                    config.numNetworkThreads,
                    config.queuedMaxRequests,
                    config.socketSendBufferBytes,
                    config.socketReceiveBufferBytes,
                    config.socketRequestMaxBytes,
                    config.maxConnectionsPerIp,
                    config.connectionsMaxIdleMs,
                    config.maxConnectionsPerIpOverrides);
            socketServer.startup();

            replicaManager = new ReplicaManager(config, time, zkClient, kafkaScheduler, logManager, isShuttingDown);

      /* start offset manager */
            offsetManager = createOffsetManager();

            kafkaController = new KafkaController(config, zkClient, brokerState);

      /* start processing requests */
            apis = new KafkaApis(socketServer.requestChannel, replicaManager, offsetManager, zkClient, config.brokerId, config, kafkaController);
            requestHandlerPool = new KafkaRequestHandlerPool(config.brokerId, socketServer.requestChannel, apis, config.numIoThreads);
            brokerState.newState(RunningAsBroker);

            Mx4jLoader.maybeLoad();

            replicaManager.startup();

            kafkaController.startup();

            topicConfigManager = new TopicConfigManager(zkClient, logManager);
            topicConfigManager.startup();

      /* tell everyone we are alive */
            kafkaHealthcheck = new KafkaHealthcheck(config.brokerId, config.advertisedHostName, config.advertisedPort, config.zkSessionTimeoutMs, zkClient);
            kafkaHealthcheck.startup();


            registerStats();
            startupComplete.set(true);
            info("started");
        } catch (Throwable e) {
            error("Fatal error during KafkaServer startup. Prepare to shutdown", e);
            shutdown();
            throw new RuntimeException(e);
        }
    }

    private ZkClient initZk() {
        info("Connecting to zookeeper on " + config.zkConnect);

        String chroot;
        if (config.zkConnect.indexOf("/") > 0)
            chroot = config.zkConnect.substring(config.zkConnect.indexOf("/"));
        else
            chroot = "";

        if (chroot.length() > 1) {
            String zkConnForChrootCreation = config.zkConnect.substring(0, config.zkConnect.indexOf("/"));
            ZkClient zkClientForChrootCreation = new ZkClient(zkConnForChrootCreation, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs, new ZKStringSerializer());
            ZkUtils.makeSurePersistentPathExists(zkClientForChrootCreation, chroot);
            info("Created zookeeper path " + chroot);
            zkClientForChrootCreation.close();
        }

        ZkClient zkClient = new ZkClient(config.zkConnect, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs, new ZKStringSerializer());
        ZkUtils.setupCommonPaths(zkClient);
        return zkClient;
    }


    /**
     * Forces some dynamic jmx beans to be registered on server startup.
     */
    private void registerStats() {
        BrokerTopicStats.getBrokerAllTopicsStats();
//        ControllerStats.uncleanLeaderElectionRate;
//        ControllerStats.leaderElectionTimer;
    }

    /**
     * Performs controlled shutdown
     */
    private void controlledShutdown() {
        if (startupComplete.get() && config.controlledShutdownEnable) {
            // We request the controller to do a controlled shutdown. On failure, we backoff for a configured period;
            // of time and try again for a configured number of retries. If all the attempt fails, we simply force;
            // the shutdown.;
            Integer remainingRetries = config.controlledShutdownMaxRetries;
            info("Starting controlled shutdown");
            BlockingChannel channel = null;
            Broker prevController = null;
            boolean shutdownSuceeded = false;
            try {
                brokerState.newState(PendingControlledShutdown);
                while (!shutdownSuceeded && remainingRetries > 0) {
                    remainingRetries = remainingRetries - 1;

                    // 1. Find the controller and establish a connection to it.;

                    // Get the current controller info. This is to ensure we use the most recent info to issue the;
                    // controlled shutdown request;
                    Integer controllerId = ZkUtils.getController(zkClient);
                    Optional<Broker> brokerOpt = ZkUtils.getBrokerInfo(zkClient, controllerId);
                    if (brokerOpt.isPresent()) {
                        Broker broker = brokerOpt.get();
                        if (channel == null || prevController == null || !prevController.equals(broker)) {
                            // if this is the first attempt or if the controller has changed, create a channel to the most recent;
                            // controller;
                            if (channel != null) {
                                channel.disconnect();
                            }
                            channel = new BlockingChannel(broker.host, broker.port,
                                    BlockingChannel.UseDefaultBufferSize,
                                    BlockingChannel.UseDefaultBufferSize,
                                    config.controllerSocketTimeoutMs);
                            channel.connect();
                            prevController = broker;
                        }
                    } else {
                        //ignore and try again;
                    }

                    // 2. issue a controlled shutdown to the controller;
                    if (channel != null) {
                        Receive response = null;
                        try {
                            // send the controlled shutdown request;
                            ControlledShutdownRequest request = new ControlledShutdownRequest(correlationId.getAndIncrement(), config.brokerId);
                            channel.send(request);

                            response = channel.receive();
                            ControlledShutdownResponse shutdownResponse = ControlledShutdownResponse.readFrom(response.buffer());
                            if (
                                    shutdownResponse.errorCode == ErrorMapping.NoError && shutdownResponse.partitionsRemaining != null &&
                                            shutdownResponse.partitionsRemaining.size() == 0) {
                                shutdownSuceeded = true;
                                info("Controlled shutdown succeeded");
                            } else {
                                info(String.format("Remaining partitions to move: %s", shutdownResponse.partitionsRemaining));
                                info(String.format("Error code from controller: %d", shutdownResponse.errorCode));
                            }
                        } catch (IOException e) {
                            channel.disconnect();
                            channel = null;
                            warn(String.format("Error during controlled shutdown, possibly because leader movement took longer than the configured socket.timeout.ms: %s", e.getMessage()));
                            // ignore and try again;
                        }
                    }
                    if (!shutdownSuceeded) {
                        Thread.sleep(config.controlledShutdownRetryBackoffMs);
                        warn("Retrying controlled shutdown after the previous attempt failed...");
                    }
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ClosedChannelException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (Throwable throwable) {
                throw new RuntimeException(throwable);
            } finally {
                if (channel != null) {
                    channel.disconnect();
                    channel = null;
                }
            }
            if (!shutdownSuceeded) {
                warn("Proceeding to do an unclean shutdown as all the controlled shutdown attempts failed");
            }
        }
    }

    /**
     * Shutdown API for shutting down a single instance of the Kafka server.
     * Shuts down the LogManager, the SocketServer and the log cleaner scheduler thread
     */
    public void shutdown() {
        try {
            info("shutting down");
            boolean canShutdown = isShuttingDown.compareAndSet(false, true);
            if (canShutdown) {
                Utils.swallow(() -> controlledShutdown());
                brokerState.newState(BrokerShuttingDown);
                if (socketServer != null)
                    Utils.swallow(() -> socketServer.shutdown());
                if (requestHandlerPool != null)
                    Utils.swallow(() -> requestHandlerPool.shutdown());
                if (offsetManager != null)
                    offsetManager.shutdown();
                Utils.swallow(() -> kafkaScheduler.shutdown());
                if (apis != null)
                    Utils.swallow(() -> apis.close());
                if (replicaManager != null)
                    Utils.swallow(() -> replicaManager.shutdown());
                if (logManager != null)
                    Utils.swallow(() -> logManager.shutdown());
                if (kafkaController != null)
                    Utils.swallow(() -> kafkaController.shutdown());
                if (zkClient != null)
                    Utils.swallow(() -> zkClient.close());

                brokerState.newState(NotRunning);
                shutdownLatch.countDown();
                startupComplete.set(false);
                info("shut down completed");
            }
        } catch (Throwable e) {
            error("Fatal error during KafkaServer shutdown.", e);
            throw e;
        }
    }

    /**
     * After calling shutdown(), use this API to wait until the shutdown is complete
     */
    public void awaitShutdown() {
        try {
            shutdownLatch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public LogManager getLogManager() {
        return logManager;
    }

    private LogManager createLogManager(ZkClient zkClient, BrokerState brokerState) throws IOException {
        LogConfig defaultLogConfig = new LogConfig(config.logSegmentBytes,
                config.logRollTimeMillis,
                config.logRollTimeJitterMillis,
                config.logFlushIntervalMessages,
                config.logFlushIntervalMs,
                config.logRetentionBytes,
                config.logRetentionTimeMillis,
                config.messageMaxBytes,
                config.logIndexSizeMaxBytes,
                config.logIndexIntervalBytes,
                config.logCleanerDeleteRetentionMs,
                config.logDeleteDelayMs,
                config.logCleanerMinCleanRatio,
                config.logCleanupPolicy.trim().toLowerCase() == "compact",
                null,
                null);
        Properties defaultProps = defaultLogConfig.toProps();
        Map<String, LogConfig> configs = Sc.mapValue(AdminUtils.fetchAllTopicConfigs(zkClient), v -> LogConfig.fromProps(defaultProps, v));
        // read the log configurations from zookeeper;
        CleanerConfig cleanerConfig = new CleanerConfig(config.logCleanerThreads,
                config.logCleanerDedupeBufferSize,
                config.logCleanerDedupeBufferLoadFactor,
                config.logCleanerIoBufferSize,
                config.messageMaxBytes,
                config.logCleanerIoMaxBytesPerSecond,
                config.logCleanerBackoffMs,
                config.logCleanerEnable, null);
        List<File> list = Sc.map(config.logDirs, d -> new File(d));
        return new LogManager(list,
                configs,
                defaultLogConfig,
                cleanerConfig,
                config.numRecoveryThreadsPerDataDir,
                config.logFlushSchedulerIntervalMs,
                config.logFlushOffsetCheckpointIntervalMs.longValue(),
                config.logCleanupIntervalMs,
                kafkaScheduler,
                brokerState,
                time);
    }

    private OffsetManager createOffsetManager() {
        OffsetManagerConfig offsetManagerConfig = new OffsetManagerConfig(
                config.offsetMetadataMaxSize,
                config.offsetsLoadBufferSize,
                config.offsetsRetentionMinutes * 60 * 1000L,
                null,
                config.offsetsTopicPartitions,
                null,
                config.offsetsTopicReplicationFactor,
                null,
                config.offsetCommitTimeoutMs,
                config.offsetCommitRequiredAcks);
        return new OffsetManager(offsetManagerConfig, replicaManager, zkClient, kafkaScheduler);
    }

}
