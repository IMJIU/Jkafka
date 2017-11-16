package kafka.server;


import com.alibaba.fastjson.JSON;
import com.google.common.collect.ImmutableMap;
import kafka.controller.ctrl.ControllerContext;
import kafka.func.Action;
import kafka.func.ActionWithThrow;
import kafka.utils.*;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;

import static kafka.utils.ZkUtils.*;

/**
 * This class handles zookeeper based leader election based on an ephemeral path. The election module does not handle
 * session expiration, instead it assumes the caller will handle it by probably try to re-elect again. If the existing
 * leader is dead, this class will handle automatic re-election and if it succeeds, it invokes the leader state change
 * callback
 */
public class ZookeeperLeaderElector extends Logging implements LeaderElector {
    public ControllerContext controllerContext;
    public String electionPath;
    public ActionWithThrow onBecomingLeader;
    public Action onResigningAsLeader;
    public Integer brokerId;

    public ZookeeperLeaderElector(ControllerContext controllerContext, String electionPath, ActionWithThrow onBecomingLeader, Action onResigningAsLeader, java.lang.Integer brokerId) {
        this.controllerContext = controllerContext;
        this.electionPath = electionPath;
        this.onBecomingLeader = onBecomingLeader;
        this.onResigningAsLeader = onResigningAsLeader;
        this.brokerId = brokerId;
        index = electionPath.lastIndexOf("/");
        if(index >0)
            makeSurePersistentPathExists(controllerContext.zkClient, electionPath.substring(0, index));
    }

    public int leaderId = -1;
    // create the election path in ZK; if one does not exist;
    public int index ;
    public LeaderChangeListener leaderChangeListener = new LeaderChangeListener();

    public void startup() {
        Utils.inLock(controllerContext.controllerLock, () -> {
            controllerContext.zkClient.subscribeDataChanges(electionPath, leaderChangeListener);
            elect();
        });
    }

    private Integer getControllerID() {
        return Sc.match(readDataMaybeNull(controllerContext.zkClient, electionPath).v1,
                (controller) -> KafkaController.parseControllerId(controller),
                () -> -1);
    }

    public Boolean elect() {
        String timestamp = Time.get().milliseconds().toString();
        String electString = JSON.toJSONString(ImmutableMap.of("version", 1, "brokerid", brokerId, "timestamp", timestamp));
        leaderId = getControllerID();
    /*
     * We can get here during the initial startup and the handleDeleted ZK callback. Because of the potential race condition,
     * it's possible that the controller has already been elected when we get here. This check will prevent the following
     * createEphemeralPath method from getting into an infinite loop if this broker is already the controller.
     */
        if (leaderId != -1) {
            debug(String.format("Broker %d has been elected as leader, so stopping the election process.", leaderId));
            return amILeader();
        }

        try {
            createEphemeralPathExpectConflictHandleZKBug(controllerContext.zkClient, electionPath, electString, brokerId,
                    (controllerString, leaderId) -> KafkaController.parseControllerId(controllerString).equals((Integer) leaderId),
                    controllerContext.zkSessionTimeout);
            info(brokerId + " successfully elected as leader");
            leaderId = brokerId;
            onBecomingLeader.invoke();
        } catch (ZkNodeExistsException e) {
            // If someone else has written the path, then;
            leaderId = getControllerID();

            if (leaderId != -1)
                debug(String.format("Broker %d was elected as leader instead of broker %d", leaderId, brokerId));
            else
                warn("A leader has been elected but just resigned, this will result in another round of election");

        } catch (Throwable e2) {
            error(String.format("Error while electing or becoming leader on broker %d", brokerId), e2);
            resign();
        }
        return amILeader();
    }

    public void close() {
        leaderId = -1;
    }

    public Boolean amILeader() {
        return leaderId == brokerId;
    }

    public void resign() {
        leaderId = -1;
        deletePath(controllerContext.zkClient, electionPath);
    }

    /**
     * We do not have session expiration listen in the ZkElection, but assuming the caller who uses this module will
     * have its own session expiration listener and handler
     */
    class LeaderChangeListener extends Logging implements IZkDataListener {
        /**
         * Called when the leader information stored in zookeeper has changed. Record the new leader in memory
         * @throws Exception On any error.
         */
//    @throws(classOf<Exception>)
        public void handleDataChange(String dataPath, Object data) {
            Utils.inLock(controllerContext.controllerLock,()->{
                leaderId = KafkaController.parseControllerId(data.toString());
                info(String.format("New leader is %d", leaderId));
            });
        }

        /**
         * Called when the leader information stored in zookeeper has been delete. Try to elect as the leader
         * @throws Exception
         *             On any error.
         */
//    @throws(classOf<Exception>)
        public void handleDataDeleted(String dataPath) {
            Utils.inLock(controllerContext.controllerLock,()-> {
                debug(String.format("%s leader change listener fired for path %s to handle data trying deleted to elect as a leader",
                    brokerId, dataPath));
                if (amILeader())
                     onResigningAsLeader.invoke();
                elect();
            });
        }
    }
}
