package kafka.utils;/**
 * Created by zhoulf on 2017/4/10.
 */

import com.yammer.metrics.core.Meter;
import kafka.annotation.threadsafe;
import kafka.metrics.KafkaMetricsGroup;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * A class to measure and throttle the rate of some process. The throttler takes a desired rate-per-second
 * (the units of the process don't matter, it could be bytes or a count of some other thing), and will sleep for
 * an appropriate amount of time when maybeThrottle() is called to attain the desired rate.
 */
@threadsafe
public class Throttler extends KafkaMetricsGroup {
    Double desiredRatePerSec;
    Long checkIntervalMs = 100L;
    Boolean throttleDown = true;
    String metricName = "throttler";
    String units;
    Time time;

    /**
     * @param desiredRatePerSec rate we want to hit in units/sec
     * @param checkIntervalMs   interval at which to check our rate
     * @param throttleDown      throttling increase or decrease our rate?
     * @param time              time implementation to use
     */
    public Throttler(Double desiredRatePerSec, Long checkIntervalMs, Boolean throttleDown, String metricName, String units, Time time) {
        this.desiredRatePerSec = desiredRatePerSec;
        this.checkIntervalMs = checkIntervalMs;
        this.throttleDown = throttleDown;
        this.metricName = metricName;
        this.units = units;
        this.time = time;
        if (this.metricName == null) {
            this.metricName = "throttler";
        }
        if(units == null){
            this.units = "entries";
        }
        periodStartNs = time.nanoseconds();
        meter = newMeter(this.metricName, this.units, TimeUnit.SECONDS);
    }
    public Throttler(Double desiredRatePerSec, Long checkIntervalMs, Time time) {
        this(desiredRatePerSec,checkIntervalMs,true,null,null,time);
    }
    private Object lock = new Object();
    private Meter meter;
    private Long periodStartNs;
    private Double observedSoFar = 0.0;

    public void maybeThrottle(Double observed) throws InterruptedException {
        meter.mark(observed.longValue());
        synchronized (lock) {
            observedSoFar += observed;
            Long now = time.nanoseconds();
            Long elapsedNs = now - periodStartNs;
            // if we have completed an interval AND we have observed something, maybe;
            // we should take a little nap;
            if (elapsedNs > checkIntervalMs * Time.NsPerMs && observedSoFar > 0) {
                double rateInSecs = (observedSoFar * Time.NsPerSec) / elapsedNs;
                boolean needAdjustment = !(throttleDown ^ (rateInSecs > desiredRatePerSec));
                if (needAdjustment) {
                    // solve for the amount of time to sleep to make us hit the desired rate;
                    long desiredRateMs = (long) (desiredRatePerSec / Time.MsPerSec.longValue());
                    long elapsedMs = elapsedNs / Time.NsPerMs;
                    long sleepTime = Math.round(observedSoFar / desiredRateMs - elapsedMs);
                    if (sleepTime > 0) {
                        trace(String.format("Natural rate is %f per second but desired rate is %f, sleeping for %d ms to compensate.", rateInSecs, desiredRatePerSec, sleepTime));
                        time.sleep(sleepTime);
                    }
                }
                periodStartNs = now;
                observedSoFar = 0d;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Random rand = new Random();
        Throttler throttler = new Throttler(100000d, 100L, true, null,null,Time.get());
        Long interval = 30000L;
        Long start = System.currentTimeMillis();
        Integer total = 0;
        while (true) {
            Integer value = rand.nextInt(1000);
            Thread.sleep(1);
            throttler.maybeThrottle(value.doubleValue());
            total += value;
            Long now = System.currentTimeMillis();
            if (now - start >= interval) {
                System.out.println(total / (interval / 1000.0));
                start = now;
                total = 0;
            }
        }
    }
}




