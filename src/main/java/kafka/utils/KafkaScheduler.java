package kafka.utils;

import kafka.annotation.threadsafe;
import kafka.func.Action;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A scheduler based on java.util.concurrent.ScheduledThreadPoolExecutor
 */
@threadsafe
public class KafkaScheduler extends Logging implements Scheduler {
    public Integer threads;
    public String threadNamePrefix = "kafka-scheduler-";
    public Boolean daemon = true;

    private volatile ScheduledThreadPoolExecutor executor = null;
    private AtomicInteger schedulerThreadId = new AtomicInteger(0);

    public KafkaScheduler() {
    }

    public KafkaScheduler(Integer threads) {
        this(threads, "kafka-scheduler-", true);
    }

    /**
     * It has a pool of kafka-scheduler- threads that do the actual work.
     *
     * @param threads          The number of threads in the thread pool
     * @param threadNamePrefix The name to use for scheduler threads. This prefix will have a number appended to it.
     * @param daemon           If true the scheduler threads will be "daemon" threads and will not block jvm shutdown.
     */
    public KafkaScheduler(Integer threads, String threadNamePrefix, Boolean daemon) {
        this.threads = threads;
        this.threadNamePrefix = threadNamePrefix;
        this.daemon = daemon;
    }

    @Override
    public void startup() {
        debug("Initializing task scheduler.");
        synchronized (this) {
            if (executor != null) {
                throw new IllegalStateException("This scheduler has already been started!");
            }
            executor = new ScheduledThreadPoolExecutor(threads);
            executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
            executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
            executor.setThreadFactory((r) -> Utils.newThread(threadNamePrefix + schedulerThreadId.getAndIncrement(), r, daemon));
        }
    }

    @Override
    public void shutdown() throws InterruptedException {
        debug("Shutting down task scheduler.");
        ensureStarted();
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.DAYS);
        this.executor = null;
    }

    public void schedule(String name, final Action action, Long delay) {
        schedule(name, action, delay, -1L, TimeUnit.MILLISECONDS);
    }

    @Override
    public void schedule(String name, final Action action, Long delay, Long period, TimeUnit unit) {
        debug(String.format("Scheduling task %s with initial delay %d ms and period %d ms.",
                name, TimeUnit.MILLISECONDS.convert(delay, unit), TimeUnit.MILLISECONDS.convert(period, unit)));
        ensureStarted();
        Runnable runnable = Utils.runnable(() -> {
            try {
                trace(String.format("Begining execution of scheduled task '%s'.", name));
                action.invoke();
            } catch (Exception e) {
                error("Uncaught exception in scheduled task '" + name + "'", e);
            } finally {
                trace(String.format("Completed execution of scheduled task '%s'.", name));
            }
        });
        if (period >= 0) {
            executor.scheduleAtFixedRate(runnable, delay, period, unit);
        } else {
            executor.schedule(runnable, delay, unit);
        }
    }

    private void ensureStarted() {
        if (executor == null)
            throw new IllegalStateException("Kafka scheduler has not been started");
    }
}
