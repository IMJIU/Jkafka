package kafka.controller;

import kafka.api.LeaderAndIsr;
import kafka.controller.ctrl.ControllerContext;
import kafka.log.TopicAndPartition;
import kafka.server.KafkaController;
import kafka.utils.Logging;
import kafka.utils.Sc;
import kafka.utils.Utils;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;

import java.util.Optional;
import java.util.Set;

import static kafka.utils.ZkUtils.getLeaderAndIsrForPartition;

/**
 * @author zhoulf
 * @create 2017-11-01 07 17
 **/
public class ReassignedPartitionsIsrChangeListener extends Logging implements IZkDataListener {
    kafka.controller.KafkaController controller;
    String topic;
    Integer partition;
    Set<Integer> reassignedReplicas;

    public ReassignedPartitionsIsrChangeListener(kafka.controller.KafkaController controller, String topic, Integer partition, Set<Integer> reassignedReplicas) {
        this.controller = controller;
        this.topic = topic;
        this.partition = partition;
        this.reassignedReplicas = reassignedReplicas;
        this.logIdent = "<ReassignedPartitionsIsrChangeListener on controller " + controller.config.brokerId + ">: ";
    }

    ZkClient zkClient = controller.controllerContext.zkClient;
    ControllerContext controllerContext = controller.controllerContext;

    /**
     * Invoked when some partitions need to move leader to preferred replica
     *
     * @throws Exception On any error.
     */
//        @throws(classOf<Exception>)
    public void handleDataChange(String dataPath, Object data) {
        Utils.inLock(controllerContext.controllerLock, () -> {
            debug(String.format("Reassigned partitions isr change listener fired for path %s with children %s", dataPath, data));
            TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
            try {
                // check if this partition is still being reassigned or not;
                ReassignedPartitionsContext reassignedPartitionContext = controllerContext.partitionsBeingReassigned.get(topicAndPartition);
                if (reassignedPartitionContext != null) {
                    // need to re-read leader and isr from zookeeper since the zkclient callback doesn't return the Stat object;
                    Optional<LeaderAndIsr> newLeaderAndIsrOpt = getLeaderAndIsrForPartition(zkClient, topic, partition);
                    Sc.match(newLeaderAndIsrOpt, (leaderAndIsr) -> { // check if new replicas have joined ISR;
                        Set<Integer> caughtUpReplicas = Sc.and(reassignedReplicas, Sc.toSet(leaderAndIsr.isr));
                        if (caughtUpReplicas == reassignedReplicas) {
                            // resume the partition reassignment process;
                            info(String.format("%d/%d replicas have caught up with the leader for partition %s being reassigned.",
                                    caughtUpReplicas.size(), reassignedReplicas.size(), topicAndPartition) +
                                    "Resuming partition reassignment");
                            controller.onPartitionReassignment(topicAndPartition, reassignedPartitionContext);
                        } else {
                            info(String.format("%d/%d replicas have caught up with the leader for partition %s being reassigned.",
                                    caughtUpReplicas.size(), reassignedReplicas.size(), topicAndPartition) +
                                    String.format("Replica(s) %s still need to catch up", Sc.subtract(reassignedReplicas, Sc.toSet(leaderAndIsr.isr))));
                        }
                    }, () -> {
                        error(String.format("Error handling reassignment of partition %s to replicas %s as it was never created",
                                topicAndPartition, reassignedReplicas));
                    });

                }
            } catch (Throwable e) {
                error("Error while handling partition reassignment", e);
            }
        });
    }

    /**
     * @throws Exception On any error.
     */
//        @throws(classOf<Exception>)
    public void handleDataDeleted(String dataPath) {
    }
}
