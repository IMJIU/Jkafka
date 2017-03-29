package kafka.log;/**
 * Created by zhoulf on 2017/3/29.
 */

import java.io.File;
import java.text.NumberFormat;


/**
 * An append-only log for storing messages.
 * <p>
 * The log is a sequence of LogSegments, each with a base offset denoting the first message in the segment.
 * <p>
 * New log segments are created according to a configurable policy that controls the size in bytes or time interval
 * for a given segment.
 *
 * @param dir           The directory in which log segments are created.
 * @param config        The log configuration settings
 * @param recoveryPoint The offset at which to begin recovery--i.e. the first offset which has not been flushed to disk
 * @param scheduler     The thread pool scheduler used for background actions
 * @param time          The time instance used for checking the clock
 */
//@threadsafe
public class Log {

    /**
     * a log file
     */
    public static final String LogFileSuffix = ".log";

    /**
     * an index file
     */
    public static final String IndexFileSuffix = ".index";

    /**
     * a file that is scheduled to be deleted
     */
    public static final String DeletedFileSuffix = ".deleted";

    /**
     * A temporary file that is being used for log cleaning
     */
    public static final String CleanedFileSuffix = ".cleaned";

    /**
     * A temporary file used when swapping files into the log
     */
    public static final String SwapFileSuffix = ".swap";

    /** Clean shutdown file that indicates the broker was cleanly shutdown in 0.8. This is required to maintain backwards compatibility
     * with 0.8 and avoid unnecessary log recovery when upgrading from 0.8 to 0.8.1 */
    /**
     * Get TODO rid of CleanShutdownFile in 0.8.2
     */
    public static final String CleanShutdownFile = ".kafka_cleanshutdown";

    /**
     * Make log segment file name from offset bytes. All this does is pad out the offset number with zeros
     * so that ls sorts the files numerically.
     *
     * @param offset The offset to use in the file name
     * @return The filename
     */
    public static String filenamePrefixFromOffset(Long offset) {
        NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(20);
        nf.setMaximumFractionDigits(0);
        nf.setGroupingUsed(false);
        return nf.format(offset);
    }

    /**
     * Construct a log file name in the given dir with the given base offset
     *
     * @param dir    The directory in which the log will reside
     * @param offset The base offset of the log file
     */
    public static File logFilename(File dir, Long offset) {
        return new File(dir, filenamePrefixFromOffset(offset) + LogFileSuffix);
    }

    /**
     * Construct an index file name in the given dir using the given base offset
     *
     * @param dir    The directory in which the log will reside
     * @param offset The base offset of the log file
     */
    public static File indexFilename(File dir, Long offset) {
        return new File(dir, filenamePrefixFromOffset(offset) + IndexFileSuffix);
    }


    /**
     * Parse the topic and partition out of the directory name of a log
     */
//    public TopicAndPartition  parseTopicPartitionName(String name)
//
//    {
//        val index = name.lastIndexOf('-')
//        TopicAndPartition(name.substring(0, index), name.substring(index + 1).toInt)
//    }
}
