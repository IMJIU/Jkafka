package kafka.utils;/**
 * Created by zhoulf on 2017/3/29.
 */

/**
 * @author
 * @create 2017-03-29 11:00
 **/
public abstract class Time {
    private static SystemTime singleInstance = new SystemTime();
    public static final Integer NsPerUs = 1000;
    public static final Integer UsPerMs = 1000;
    public static final Integer MsPerSec = 1000;
    public static final Integer NsPerMs = NsPerUs * UsPerMs;
    public static final Integer NsPerSec = NsPerMs * MsPerSec;
    public static final Integer UsPerSec = UsPerMs * MsPerSec;
    public static final Integer SecsPerMin = 60;
    public static final Integer MinsPerHour = 60;
    public static final Integer HoursPerDay = 24;
    public static final Integer SecsPerHour = SecsPerMin * MinsPerHour;
    public static final Integer SecsPerDay = SecsPerHour * HoursPerDay;
    public static final Integer MinsPerDay = MinsPerHour * HoursPerDay;

    public abstract Long milliseconds();

    public abstract Long nanoseconds();

    public abstract void sleep(Long ms) throws InterruptedException;

    public static SystemTime get() {
        return singleInstance;
    }
}
