package kafka.utils;/**
 * Created by zhoulf on 2017/3/29.
 */

/**
 * @author
 * @create 2017-03-29 11:00
 **/
public class KafkaTime extends Time {

    public Long milliseconds() {
        return System.currentTimeMillis();
    }


    public Long nanoseconds() {
        return System.nanoTime();
    }

    public void sleep(Long ms) throws InterruptedException {
        Thread.sleep(ms);
    }
}
