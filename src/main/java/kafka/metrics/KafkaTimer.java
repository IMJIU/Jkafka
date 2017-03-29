package kafka.metrics;

import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import kafka.func.Processor;

/**
 * Created by Administrator on 2017/3/29.
 */
public class KafkaTimer {
    public Timer metric;

    public KafkaTimer(Timer metric) {
        this.metric = metric;
    }

    public <A> A time(Processor<A> p) {
        TimerContext ctx = metric.time();
        try {
            return p.process();
        } finally {
            ctx.stop();
        }
    }
}
