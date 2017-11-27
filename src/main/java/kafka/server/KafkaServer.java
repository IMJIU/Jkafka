package kafka.server;

/**
 * @author zhoulf
 * @create 2017-11-27 59 17
 **/

import com.yammer.metrics.core.Gauge;
import kafka.controller.KafkaController;
import kafka.log.LogManager;
import kafka.metrics.KafkaMetricsGroup;
import kafka.network.SocketServer;
import kafka.utils.KafkaScheduler;
import kafka.utils.Time;
import org.I0Itec.zkclient.ZkClient;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
    BrokerState brokerState = new BrokerState;
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
        } catch {
            case Throwable e ->
                    fatal("Fatal error during KafkaServer startup. Prepare to shutdown", e);
                shutdown();
                throw e;
        }
    }

    private ZkClient initZk() {
        info("Connecting to zookeeper on " + config.zkConnect);

        val chroot = {
        if (config.zkConnect.indexOf("/") > 0)
            config.zkConnect.substring(config.zkConnect.indexOf("/"));
        else ;
        "";
        }

        if (chroot.length > 1) {
            val zkConnForChrootCreation = config.zkConnect.substring(0, config.zkConnect.indexOf("/"));
            val zkClientForChrootCreation = new ZkClient(zkConnForChrootCreation, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs, ZKStringSerializer);
            ZkUtils.makeSurePersistentPathExists(zkClientForChrootCreation, chroot);
            info("Created zookeeper path " + chroot);
            zkClientForChrootCreation.close();
        }

        val zkClient = new ZkClient(config.zkConnect, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs, ZKStringSerializer);
        ZkUtils.setupCommonPaths(zkClient);
        zkClient;
    }


    /**
     * Forces some dynamic jmx beans to be registered on server startup.
     */
    private void registerStats() {
        BrokerTopicStats.getBrokerAllTopicsStats();
        ControllerStats.uncleanLeaderElectionRate;
        ControllerStats.leaderElectionTimer;
    }

    /**
     * Performs controlled shutdown
     */
    private void controlledShutdown() {
        if (startupComplete.get() && config.controlledShutdownEnable) {
            // We request the controller to do a controlled shutdown. On failure, we backoff for a configured period;
            // of time and try again for a configured number of retries. If all the attempt fails, we simply force;
            // the shutdown.;
            var remainingRetries = config.controlledShutdownMaxRetries;
            info("Starting controlled shutdown");
            var channel :BlockingChannel = null;
            var prevController :Broker = null;
            var shutdownSuceeded :Boolean = false;
            try {
                brokerState.newState(PendingControlledShutdown);
                while (!shutdownSuceeded && remainingRetries > 0) {
                    remainingRetries = remainingRetries - 1;

                    // 1. Find the controller and establish a connection to it.;

                    // Get the current controller info. This is to ensure we use the most recent info to issue the;
                    // controlled shutdown request;
                    val controllerId = ZkUtils.getController(zkClient);
                    ZkUtils.getBrokerInfo(zkClient, controllerId) match {
                        case Some(broker) ->
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
                        case None ->
                            //ignore and try again;
                    }

                    // 2. issue a controlled shutdown to the controller;
                    if (channel != null) {
                        var Receive response = null;
                        try {
                            // send the controlled shutdown request;
                            val request = new ControlledShutdownRequest(correlationId.getAndIncrement, config.brokerId);
                            channel.send(request);

                            response = channel.receive();
                            val shutdownResponse = ControlledShutdownResponse.readFrom(response.buffer);
                            if (
                            shutdownResponse.errorCode == ErrorMapping.NoError && shutdownResponse.partitionsRemaining != null &&;
                            shutdownResponse.partitionsRemaining.size == 0){
                                shutdownSuceeded = true;
                                info("Controlled shutdown succeeded");
                            }
        else{
                                info(String.format("Remaining partitions to move: %s", shutdownResponse.partitionsRemaining.mkString(",")))
                                info(String.format("Error code from controller: %d", shutdownResponse.errorCode))
                            }
                        } catch {
                            case java ioe.io.IOException ->
                                channel.disconnect();
                                channel = null;
                                warn(String.format("Error during controlled shutdown, possibly because leader movement took longer than the configured socket.timeout.ms: %s", ioe.getMessage))
                                // ignore and try again;
                        }
                    }
                    if (!shutdownSuceeded) {
                        Thread.sleep(config.controlledShutdownRetryBackoffMs);
                        warn("Retrying controlled shutdown after the previous attempt failed...");
                    }
                }
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
            val canShutdown = isShuttingDown.compareAndSet(false, true);
            if (canShutdown) {
                Utils.swallow(controlledShutdown());
                brokerState.newState(BrokerShuttingDown);
                if (socketServer != null)
                    Utils.swallow(socketServer.shutdown());
                if (requestHandlerPool != null)
                    Utils.swallow(requestHandlerPool.shutdown());
                if (offsetManager != null)
                    offsetManager.shutdown();
                Utils.swallow(kafkaScheduler.shutdown());
                if (apis != null)
                    Utils.swallow(apis.close());
                if (replicaManager != null)
                    Utils.swallow(replicaManager.shutdown());
                if (logManager != null)
                    Utils.swallow(logManager.shutdown());
                if (kafkaController != null)
                    Utils.swallow(kafkaController.shutdown());
                if (zkClient != null)
                    Utils.swallow(zkClient.close());

                brokerState.newState(NotRunning);
                shutdownLatch.countDown();
                startupComplete.set(false);
                info("shut down completed");
            }
        } catch {
            case Throwable e ->
                    fatal("Fatal error during KafkaServer shutdown.", e);
                throw e;
        }
    }

    /**
     * After calling shutdown(), use this API to wait until the shutdown is complete
     */
    public Unit

    void awaitShutdown() shutdownLatch.

    await();

    public LogManager

    void getLogManager() logManager;

    private LogManager

    void createLogManager(ZkClient zkClient, BrokerState brokerState) {
        val defaultLogConfig = LogConfig(segmentSize = config.logSegmentBytes,
                segmentMs = config.logRollTimeMillis,
                segmentJitterMs = config.logRollTimeJitterMillis,
                flushInterval = config.logFlushIntervalMessages,
                flushMs = config.logFlushIntervalMs.toLong,
                retentionSize = config.logRetentionBytes,
                retentionMs = config.logRetentionTimeMillis,
                maxMessageSize = config.messageMaxBytes,
                maxIndexSize = config.logIndexSizeMaxBytes,
                indexInterval = config.logIndexIntervalBytes,
                deleteRetentionMs = config.logCleanerDeleteRetentionMs,
                fileDeleteDelayMs = config.logDeleteDelayMs,
                minCleanableRatio = config.logCleanerMinCleanRatio,
                compact = config.logCleanupPolicy.trim.toLowerCase == "compact");
        val defaultProps = defaultLogConfig.toProps;
        val configs = AdminUtils.fetchAllTopicConfigs(zkClient).mapValues(LogConfig.fromProps(defaultProps, _));
        // read the log configurations from zookeeper;
        val cleanerConfig = CleanerConfig(numThreads = config.logCleanerThreads,
                dedupeBufferSize = config.logCleanerDedupeBufferSize,
                dedupeBufferLoadFactor = config.logCleanerDedupeBufferLoadFactor,
                ioBufferSize = config.logCleanerIoBufferSize,
                maxMessageSize = config.messageMaxBytes,
                maxIoBytesPerSecond = config.logCleanerIoMaxBytesPerSecond,
                backOffMs = config.logCleanerBackoffMs,
                enableCleaner = config.logCleanerEnable);
        new LogManager(logDirs = config.logDirs.map(new File(_)).toArray,
                topicConfigs = configs,
                defaultConfig = defaultLogConfig,
                cleanerConfig = cleanerConfig,
                ioThreads = config.numRecoveryThreadsPerDataDir,
                flushCheckMs = config.logFlushSchedulerIntervalMs,
                flushCheckpointMs = config.logFlushOffsetCheckpointIntervalMs,
                retentionCheckMs = config.logCleanupIntervalMs,
                scheduler = kafkaScheduler,
                brokerState = brokerState,
                time = time);
    }

    private OffsetManager

    void createOffsetManager() {
        val offsetManagerConfig = OffsetManagerConfig(
                maxMetadataSize = config.offsetMetadataMaxSize,
                loadBufferSize = config.offsetsLoadBufferSize,
                offsetsRetentionMs = config.offsetsRetentionMinutes * 60 * 1000L,
                offsetsTopicNumPartitions = config.offsetsTopicPartitions,
                offsetsTopicReplicationFactor = config.offsetsTopicReplicationFactor,
                offsetCommitTimeoutMs = config.offsetCommitTimeoutMs,
                offsetCommitRequiredAcks = config.offsetCommitRequiredAcks);
        new OffsetManager(offsetManagerConfig, replicaManager, zkClient, kafkaScheduler);
    }

}
