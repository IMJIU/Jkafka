package kafka.metrics;

import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import kafka.func.ActionWithResult;

/**
 * Created by Administrator on 2017/3/29.
 */
public class KafkaTimer {
    public Timer metric;

    public KafkaTimer(Timer metric) {
        this.metric = metric;
    }

    public <A> A time(ActionWithResult<A> p) {
        TimerContext ctx = metric.time();
        try {
            return p.invoke();
        } finally {
            ctx.stop();
        }
    }
}
