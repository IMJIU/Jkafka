package kafka.utils;

/**
 * @author zhoulf
 * @create 2017-10-24 31 18
 **/

class DelayedItem<T>(val T item, Long delay, TimeUnit unit) extends Delayed with Logging {

        val createdMs = SystemTime.milliseconds;
        val delayMs = {
        val given = unit.toMillis(delay);
        if (given < 0 || (createdMs + given) < 0) (Long.MAX_VALUE - createdMs)
        else given;
        }

       public void this(T item, Long delayMs) =
        this(item, delayMs, TimeUnit.MILLISECONDS);

        /**
         * The remaining delay time
         */
       public Long  void getDelay(TimeUnit unit) {
        val elapsedMs = (SystemTime.milliseconds - createdMs);
        unit.convert(max(delayMs - elapsedMs, 0), TimeUnit.MILLISECONDS);
        }

       public Integer  void compareTo(Delayed d) {
        val delayed = d.asInstanceOf<DelayedItem[T]>
        val myEnd = createdMs + delayMs;
        val yourEnd = delayed.createdMs + delayed.delayMs;

        if(myEnd < yourEnd) -1;
        else if(myEnd > yourEnd) 1;
        else 0;
        }

        }
