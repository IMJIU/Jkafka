package kafka.controller.ctrl;

import com.yammer.metrics.core.Meter;
import kafka.metrics.KafkaMetricsGroup;
import kafka.metrics.KafkaTimer;

import java.util.concurrent.TimeUnit;

/**
 * @author zhoulf
 * @create 2017-11-06 14:10
 **/
public class ControllerStats extends KafkaMetricsGroup {
    public static final Meter uncleanLeaderElectionRate = newMeter("UncleanLeaderElectionsPerSec", "elections", TimeUnit.SECONDS);
    public static final KafkaTimer leaderElectionTimer = new KafkaTimer(newTimer("LeaderElectionRateAndTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS));
}
