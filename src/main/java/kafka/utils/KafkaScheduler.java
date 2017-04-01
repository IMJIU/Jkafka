package kafka.utils;

import kafka.annotation.threadsafe;

/**
 * A scheduler based on java.util.concurrent.ScheduledThreadPoolExecutor
 *
 * It has a pool of kafka-scheduler- threads that do the actual work.
 *
 * @param threads The number of threads in the thread pool
 * @param threadNamePrefix The name to use for scheduler threads. This prefix will have a number appended to it.
 * @param daemon If true the scheduler threads will be "daemon" threads and will not block jvm shutdown.
 */
@threadsafe
public class KafkaScheduler extends Logging implements Scheduler{
    public Integer threads;
    public  String threadNamePrefix = "kafka-scheduler-";
    public Boolean daemon = true;

  @volatile private var ScheduledThreadPoolExecutor executor = null
        private val schedulerThreadId = new AtomicInteger(0);

        override public void  startup() {
            debug("Initializing task scheduler.");
            this synchronized {
                if(executor != null)
                    throw new IllegalStateException("This scheduler has already been started!");
                executor = new ScheduledThreadPoolExecutor(threads);
                executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
                executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
                executor.setThreadFactory(new ThreadFactory() {
                    public void  newThread(Runnable runnable): Thread =
                            Utils.newThread(threadNamePrefix + schedulerThreadId.getAndIncrement(), runnable, daemon);
                });
            }
        }

        override public void  shutdown() {
            debug("Shutting down task scheduler.");
            ensureStarted;
            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.DAYS);
            this.executor = null;
        }

    public void  schedule(String name, fun: ()=>Unit, Long delay, Long period, TimeUnit unit) = {
        debug("Scheduling task %s with initial delay %d ms and period %d ms.";
                    .format(name, TimeUnit.MILLISECONDS.convert(delay, unit), TimeUnit.MILLISECONDS.convert(period, unit)))
        ensureStarted;
        val runnable = Utils.runnable {
            try {
                trace(String.format("Begining execution of scheduled task '%s'.",name))
                fun();
            } catch {
                case Throwable t => error("Uncaught exception in scheduled task '" + name +"'", t);
            } finally {
                trace(String.format("Completed execution of scheduled task '%s'.",name))
            }
        }
        if(period >= 0)
            executor.scheduleAtFixedRate(runnable, delay, period, unit);
        else;
        executor.schedule(runnable, delay, unit);
    }

    private public void  ensureStarted = {
        if(executor == null)
            throw new IllegalStateException("Kafka scheduler has not been started");
}
    }
}
