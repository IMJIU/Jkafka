package kafka.log;

/**
 * Created by zhoulf on 2017/4/12.
 */
public enum LogCleaningState {
    LogCleaningInProgress,
    LogCleaningAborted,
    LogCleaningPaused
}
