package kafka.utils;

import java.util.concurrent.TimeUnit;

/**
 * A class used for unit testing things which depend on the Time interface.
 * <p>
 * This class never manually advances the clock, it only does so when you call
 * sleep(ms)
 * <p>
 * It also comes with an associated scheduler instance for managing background tasks in
 * a deterministic way.
 */
public class MockTime extends Time {

    private static volatile Long currentMs;
    public static MockScheduler scheduler;

    public MockTime(java.lang.Long currentMs) {
        this.currentMs = currentMs;
        scheduler = new MockScheduler(this);
    }

    public MockTime() {
        this(System.currentTimeMillis());
        scheduler = new MockScheduler(this);
    }

    public static Long milliseconds() {
        return currentMs;
    }

    public static Long nanoseconds() {
        return TimeUnit.NANOSECONDS.convert(currentMs, TimeUnit.MILLISECONDS);
    }


    public static void sleep(Long ms) {
        currentMs += ms;
        scheduler.tick();
    }

    @Override
    public String toString() {
        return String.format("MockTime(%d)", milliseconds());
    }
}
