package kafka.producer;

/**
 * @author zhoulf
 * @create 2017-12-01 25 18
 **/


public class ProducerPool {
        /**
         * Used in ProducerPool to initiate a SyncProducer connection with a broker.
         */
       public static SyncProducer   createSyncProducer(ProducerConfig config, Broker broker) {
        val props = new Properties();
        props.put("host", broker.host);
        props.put("port", broker.port.toString);
        props.putAll(config.props.props);
        new SyncProducer(new SyncProducerConfig(props));
        }
        }

class ProducerPool(val ProducerConfig config) extends Logging {
private val syncProducers = new HashMap<Integer, SyncProducer>
private val lock = new Object();

       public void updateProducer(Seq topicMetadata<TopicMetadata>) {
        val newBrokers = new collection.mutable.HashSet<Broker>
        topicMetadata.foreach(tmd -> {
        tmd.partitionsMetadata.foreach(pmd -> {
        if(pmd.leader.isDefined)
        newBrokers+=(pmd.leader.get);
        });
        });
        lock synchronized {
        newBrokers.foreach(b -> {
        if(syncProducers.contains(b.id)){
        syncProducers(b.id).close();
        syncProducers.put(b.id, ProducerPool.createSyncProducer(config, b));
        } else;
        syncProducers.put(b.id, ProducerPool.createSyncProducer(config, b));
        });
        }
        }

       public SyncProducer  void getProducer(Int brokerId)  {
        lock.synchronized {
        val producer = syncProducers.get(brokerId);
        producer match {
        case Some(p) -> p;
        case None -> throw new UnavailableProducerException(String.format("Sync producer for broker id %d does not exist",brokerId))
        }
        }
        }

        /**
         * Closes all the producers in the pool
         */
       public void close() = {
        lock.synchronized {
        info("Closing all sync producers");
        val iter = syncProducers.values.iterator;
        while(iter.hasNext);
        iter.next.close;
        }
        }
        }
