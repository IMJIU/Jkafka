package kafka.controller;

/**
 * @author zhoulf
 * @create 2017-11-07 18:35
 **/
public enum ReplicaState {
    NewReplica((byte) 1), OnlineReplica((byte) 2), OfflineReplica((byte) 3),
    ReplicaDeletionStarted((byte) 4), ReplicaDeletionSuccessful((byte) 5),
    ReplicaDeletionIneligible((byte) 6), NonExistentReplica((byte) 7);
    byte state;

    ReplicaState(byte state) {
        this.state = state;
    }
}