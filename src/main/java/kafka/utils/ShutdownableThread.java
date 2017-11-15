package kafka.utils;/**
 * Created by zhoulf on 2017/4/13.
 */

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author
 * @create 2017-04-13 44 10
 **/
public abstract class ShutdownableThread extends Thread {
    public String name;
    public Boolean isInterruptible = true;
    public Logging logger = new Logging();

    public ShutdownableThread(String name) {
        this(name, true);
    }

    public ShutdownableThread(String name, Boolean isInterruptible) {
        this.name = name;
        this.isInterruptible = isInterruptible;
        this.setDaemon(false);
        logger.logIdent = "[" + name + "], ";
    }


    public AtomicBoolean isRunning = new AtomicBoolean(true);
    private CountDownLatch shutdownLatch = new CountDownLatch(1);

    public void shutdown()  {
        initiateShutdown();
        awaitShutdown();
    }

    public Boolean initiateShutdown() {
        if (isRunning.compareAndSet(true, false)) {
            logger.info("Shutting down");
            isRunning.set(false);
            if (isInterruptible)
                interrupt();
            return true;
        } else ;
        return false;
    }

    /**
     * After calling initiateShutdown(), use this API to wait until the shutdown is complete
     */
    public void awaitShutdown()  {
        try {
            shutdownLatch.await();
        } catch (InterruptedException e) {
            logger.error(e.getMessage(),e);
        }
        logger.info("Shutdown completed");
    }

    public abstract void doWork();

    @Override
    public void run() {
        logger.info("Starting ");
        try {
            while (isRunning.get()) {
                doWork();
            }
        } catch (Exception e) {
            if (isRunning.get())
                logger.error("Error due to ", e);
        }
        shutdownLatch.countDown();
        logger.info("Stopped ");
    }
}
