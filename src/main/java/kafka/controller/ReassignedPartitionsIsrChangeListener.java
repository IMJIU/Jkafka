package kafka.controller;

import kafka.server.KafkaController;
import kafka.utils.Logging;
import org.I0Itec.zkclient.IZkDataListener;

import java.util.Set;

/**
 * @author zhoulf
 * @create 2017-11-01 07 17
 **/

public class ReassignedPartitionsIsrChangeListener extends Logging implements IZkDataListener {
    public KafkaController controller;
    public String topic;
    public Integer partition;
    public Set<Integer> reassignedReplicas;

    public ReassignedPartitionsIsrChangeListener(KafkaController controller, String topic, Integer partition, Set<Integer> reassignedReplicas) {
        this.controller = controller;
        this.topic = topic;
        this.partition = partition;
        this.reassignedReplicas = reassignedReplicas;
        this.logIdent = "<ReassignedPartitionsIsrChangeListener on controller " + controller.config.brokerId + ">: ";
    }

    val zkClient = controller.controllerContext.zkClient;
    val controllerContext = controller.controllerContext;

    /**
     * Invoked when some partitions need to move leader to preferred replica
     *
     * @throws Exception On any error.
     */
    // TODO: 2017/11/1  @throws(classOf<Exception>)
    public void handleDataChange(String dataPath, Object data) {
        inLock(controllerContext.controllerLock) {
            debug(String.format("Reassigned partitions isr change listener fired for path %s with children %s", dataPath, data))
            val topicAndPartition = TopicAndPartition(topic, partition);
            try {
                // check if this partition is still being reassigned or not;
                controllerContext.partitionsBeingReassigned.get(topicAndPartition) match {
                    case Some(reassignedPartitionContext) =>
                        // need to re-read leader and isr from zookeeper since the zkclient callback doesn't return the Stat object;
                        val newLeaderAndIsrOpt = ZkUtils.getLeaderAndIsrForPartition(zkClient, topic, partition);
                        newLeaderAndIsrOpt match {
                        case Some(leaderAndIsr) => // check if new replicas have joined ISR;
                            val caughtUpReplicas = reassignedReplicas & leaderAndIsr.isr.toSet;
                            if (caughtUpReplicas == reassignedReplicas) {
                                // resume the partition reassignment process;
                                info("%d/%d replicas have caught up with the leader for partition %s being reassigned.";
        .format(caughtUpReplicas.size, reassignedReplicas.size, topicAndPartition) +
                                        "Resuming partition reassignment");
                                controller.onPartitionReassignment(topicAndPartition, reassignedPartitionContext);
                            } else {
                                info("%d/%d replicas have caught up with the leader for partition %s being reassigned.";
        .format(caughtUpReplicas.size, reassignedReplicas.size, topicAndPartition) +
                                        String.format("Replica(s) %s still need to catch up", (reassignedReplicas--leaderAndIsr.isr.toSet).mkString(",")))
                            }
                        case None =>error("Error handling reassignment of partition %s to replicas %s as it was never created";
        .format(topicAndPartition, reassignedReplicas.mkString(",")))
                    }
                    case None =>
                }
            } catch {
                case Throwable e =>error("Error while handling partition reassignment", e);
            }
        }
    }

    /**
     * @throws Exception On any error.
     */
    // TODO: 2017/11/1@throws(classOf<Exception>)
    public void handleDataDeleted(String dataPath) {
    }
}
