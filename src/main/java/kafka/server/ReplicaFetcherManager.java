package kafka.server;


import kafka.cluster.Broker;

public class ReplicaFetcherManager extends AbstractFetcherManager {//"ReplicaFetcherManager on broker " + brokerConfig.brokerId,"Replica", brokerConfig.numReplicaFetchers
    private KafkaConfig brokerConfig;
    private ReplicaManager replicaMgr;

    public ReplicaFetcherManager(String name, String clientId, Integer numFetchers, KafkaConfig brokerConfig, ReplicaManager replicaMgr) {
        super("ReplicaFetcherManager on broker " + brokerConfig.brokerId, clientId, brokerConfig.numReplicaFetchers);
        this.brokerConfig = brokerConfig;
        this.replicaMgr = replicaMgr;
    }

    @Override
    public AbstractFetcherThread createFetcherThread(Integer fetcherId, Broker sourceBroker) {
        return new ReplicaFetcherThread(String.format("ReplicaFetcherThread-%d-%d", fetcherId, sourceBroker.id), sourceBroker, brokerConfig, replicaMgr);
    }

    public void shutdown() {
        info("shutting down");
        closeAllFetchers();
        info("shutdown completed");
    }
}
