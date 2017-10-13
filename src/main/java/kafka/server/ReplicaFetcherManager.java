package kafka.server;


class ReplicaFetcherManager(private val KafkaConfig brokerConfig, private val ReplicaManager replicaMgr);
        extends AbstractFetcherManager("ReplicaFetcherManager on broker " + brokerConfig.brokerId,
        "Replica", brokerConfig.numReplicaFetchers) {

        overridepublic AbstractFetcherThread  void createFetcherThread(Int fetcherId, Broker sourceBroker) {
        new ReplicaFetcherThread(String.format("ReplicaFetcherThread-%d-%d",fetcherId, sourceBroker.id), sourceBroker, brokerConfig, replicaMgr)
        }

       public void shutdown() {
        info("shutting down");
        closeAllFetchers();
        info("shutdown completed");
        }
        }
