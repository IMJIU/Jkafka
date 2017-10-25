package kafka.utils;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * @author zhoulf
 * @create 2017-10-24 31 18
 **/

public class DelayedItem<T> extends Logging implements Delayed {
    public T item;
    public Long delay;
    public TimeUnit unit;

    public DelayedItem(T item, Long delayMs) {
        this(item, delayMs, TimeUnit.MILLISECONDS);
    }

    public DelayedItem(T item, Long delay, TimeUnit unit) {
        this.item = item;
        this.delay = delay;
        this.unit = unit;
        init();
    }

    public void init() {
        long given = unit.toMillis(delay);
        if (given < 0 || (createdMs + given) < 0) {
            delayMs = (Long.MAX_VALUE - createdMs);
        } else {
            delayMs = given;
        }
    }

    public long createdMs = SystemTime.get().milliseconds();
    public long delayMs;


    /**
     * The remaining delay time
     */
    @Override
    public long getDelay(TimeUnit unit) {
        long elapsedMs = (SystemTime.get().milliseconds() - createdMs);
        return unit.convert(Math.max(delayMs - elapsedMs, 0), TimeUnit.MILLISECONDS);
    }

    public int compareTo(Delayed d) {
        DelayedItem<T> delayed = (DelayedItem)d;
                long myEnd = createdMs + delayMs;
        long yourEnd = delayed.createdMs + delayed.delayMs;

        if (myEnd < yourEnd) return -1;
        else if (myEnd > yourEnd) return 1;
        else return 0;
    }

}
