package kafka.controller.partition;

/**
 * @author zhoulf
 * @create 2017-11-06 16:58
 **/
public enum PartitionState {
    NewPartition((byte) 0), OnlinePartition((byte) 1), OfflinePartition((byte) 2), NonExistentPartition((byte) 3);
    private byte state;

    PartitionState(byte state) {
        this.state = state;
    }
}
