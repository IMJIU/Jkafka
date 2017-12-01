package kafka.producer;

/**
 * A partitioner controls the mapping between user-provided keys and kafka partitions. Users can implement a custom
 * partitioner to change this mapping.
 * <p>
 * Implementations will be constructed via reflection and are required to have a constructor that takes a single
 * VerifiableProperties instance--this allows passing configuration properties into the partitioner implementation.
 */
public interface Partitioner {


    /**
     * Uses the key to calculate a partition bucket id for routing
     * the data to the appropriate broker partition
     *
     * @return an integer between 0 and numPartitions-1
     */
    Integer partition(Object key, Integer numPartitions);
}