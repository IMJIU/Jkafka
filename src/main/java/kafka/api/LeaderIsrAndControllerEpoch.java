package kafka.api;

/**
 * @author zhoulf
 * @create 2017-10-19 46 12
 **/

public class LeaderIsrAndControllerEpoch {
    public LeaderAndIsr leaderAndIsr;
    public Integer controllerEpoch;

    public LeaderIsrAndControllerEpoch(LeaderAndIsr leaderAndIsr, Integer controllerEpoch) {
        this.leaderAndIsr = leaderAndIsr;
        this.controllerEpoch = controllerEpoch;
    }

    @Override
    public String toString() {
        StringBuilder leaderAndIsrInfo = new StringBuilder();
        leaderAndIsrInfo.append("(Leader:" + leaderAndIsr.leader);
        leaderAndIsrInfo.append(",ISR:" + leaderAndIsr.isr);
        leaderAndIsrInfo.append(",LeaderEpoch:" + leaderAndIsr.leaderEpoch);
        leaderAndIsrInfo.append(",ControllerEpoch:" + controllerEpoch + ")");
        return leaderAndIsrInfo.toString();
    }
}
