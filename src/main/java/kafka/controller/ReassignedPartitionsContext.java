package kafka.controller;

import java.util.List;

/**
 * @author zhoulf
 * @create 2017-11-01 17:06
 **/
public class ReassignedPartitionsContext {
    public List<Integer> newReplicas;
    public ReassignedPartitionsIsrChangeListener isrChangeListener;

    public ReassignedPartitionsContext(List<Integer> newReplicas) {
        this.newReplicas = newReplicas;
    }

    public ReassignedPartitionsContext(List<Integer> newReplicas, ReassignedPartitionsIsrChangeListener isrChangeListener) {
        this.newReplicas = newReplicas;
        this.isrChangeListener = isrChangeListener;
    }
}
