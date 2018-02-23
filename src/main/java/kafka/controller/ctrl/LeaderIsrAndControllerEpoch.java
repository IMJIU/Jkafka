package kafka.controller.ctrl;

import kafka.api.LeaderAndIsr;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LeaderIsrAndControllerEpoch that = (LeaderIsrAndControllerEpoch) o;

        if (leaderAndIsr != null ? !leaderAndIsr.equals(that.leaderAndIsr) : that.leaderAndIsr != null) return false;
        return controllerEpoch != null ? controllerEpoch.equals(that.controllerEpoch) : that.controllerEpoch == null;
    }

    @Override
    public int hashCode() {
        int result = leaderAndIsr != null ? leaderAndIsr.hashCode() : 0;
        result = 31 * result + (controllerEpoch != null ? controllerEpoch.hashCode() : 0);
        return result;
    }
}
