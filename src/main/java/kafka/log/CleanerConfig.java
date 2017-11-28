package kafka.log;

/**
 * @author zhoulf
 * @create 2017-11-28 39 13
 **/

/**
 * Configuration parameters for the log cleaner
 *
 * @param numThreads             The number of cleaner threads to run
 * @param dedupeBufferSize       The total memory used for log deduplication
 * @param dedupeBufferLoadFactor The maximum percent full for the deduplication buffer
 * @param maxMessageSize         The maximum size of a message that can appear in the log
 * @param maxIoBytesPerSecond    The maximum read and write I/O that all cleaner threads are allowed to do
 * @param backOffMs              The amount of time to wait before rechecking if no logs are eligible for cleaning
 * @param enableCleaner          Allows completely disabling the log cleaner
 * @param hashAlgorithm          The hash algorithm to use in key comparison.
 */
public class CleanerConfig {
    public Integer numThreads = 1;
    public Long dedupeBufferSize = 4 * 1024 * 1024L;
    public Double dedupeBufferLoadFactor = 0.9d;
    public Integer ioBufferSize = 1024 * 1024;
    public Integer maxMessageSize = 32 * 1024 * 1024;
    public Double maxIoBytesPerSecond = java.lang.Double.MAX_VALUE;
    public Long backOffMs = 15 * 1000L;
    public Boolean enableCleaner = true;
    public String hashAlgorithm = "MD5";

    /**
     * Configuration parameters for the log cleaner
     *
     * @param numThreads             The number of cleaner threads to run
     * @param dedupeBufferSize       The total memory used for log deduplication
     * @param dedupeBufferLoadFactor The maximum percent full for the deduplication buffer
     * @param maxMessageSize         The maximum size of a message that can appear in the log
     * @param maxIoBytesPerSecond    The maximum read and write I/O that all cleaner threads are allowed to do
     * @param backOffMs              The amount of time to wait before rechecking if no logs are eligible for cleaning
     * @param enableCleaner          Allows completely disabling the log cleaner
     * @param hashAlgorithm          The hash algorithm to use in key comparison.
     */
    public CleanerConfig(Integer numThreads, Long dedupeBufferSize, Double dedupeBufferLoadFactor, Integer ioBufferSize, Integer maxMessageSize, Double maxIoBytesPerSecond, Long backOffMs, Boolean enableCleaner, String hashAlgorithm) {
        this.numThreads = numThreads;
        this.dedupeBufferSize = dedupeBufferSize;
        this.dedupeBufferLoadFactor = dedupeBufferLoadFactor;
        this.ioBufferSize = ioBufferSize;
        this.maxMessageSize = maxMessageSize;
        this.maxIoBytesPerSecond = maxIoBytesPerSecond;
        this.backOffMs = backOffMs;
        this.enableCleaner = enableCleaner;
        this.hashAlgorithm = hashAlgorithm == null ? "MD5" : hashAlgorithm;
    }

    public CleanerConfig() {
    }

    public CleanerConfig(Boolean enableCleaner) {
        this.enableCleaner = enableCleaner;
    }
}
