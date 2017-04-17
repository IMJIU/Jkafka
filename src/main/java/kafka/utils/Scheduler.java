package kafka.utils;

import kafka.func.Action;

import java.util.concurrent.TimeUnit;

/**
 * A scheduler for running jobs
 * <p>
 * This interface controls a job scheduler that allows scheduling either repeating background jobs
 * that execute periodically or delayed one-time actions that are scheduled in the future.
 */
public interface Scheduler {


    /**
     * Initialize this scheduler so it is ready to accept scheduling of tasks
     */
    void startup();

    /**
     * Shutdown this scheduler. When this method is complete no more executions of background tasks will occur.
     * This includes tasks scheduled with a delayed execution.
     */
    void shutdown() throws InterruptedException;

    /**
     * Schedule a task
     *
     * @param name   The name of this task
     * @param delay  The amount of time to wait before the first execution
     * @param period The period with which to execute the task. If < 0 the task will execute only once.
     * @param unit   The unit for the preceding times.
     */
    void schedule(String name, Action action, Long delay, Long period, TimeUnit unit);

    void schedule(String name, final Action action, Long delay);
}
