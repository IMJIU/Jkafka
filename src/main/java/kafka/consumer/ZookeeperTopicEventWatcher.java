package kafka.consumer;

import kafka.utils.Logging;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.Watcher;
import java.util.List;

/**
 * @author zhoulf
 * @create 2017-12-14 45 17
 **/

public class ZookeeperTopicEventWatcher extends Logging {
    public ZkClient zkClient;
    public TopicEventHandler<String> eventHandler;

    public ZookeeperTopicEventWatcher(ZkClient zkClient, TopicEventHandler<String> eventHandler) {
        this.zkClient = zkClient;
        this.eventHandler = eventHandler;
        startWatchingTopicEvents();
    }

    public Object lock = new Object();


    private void startWatchingTopicEvents() {
        ZkTopicEventListener topicEventListener = new ZkTopicEventListener();
        ZkUtils.makeSurePersistentPathExists(zkClient, ZkUtils.BrokerTopicsPath);

        zkClient.subscribeStateChanges(new ZkSessionExpireListener(topicEventListener));

        List<String> topics = zkClient.subscribeChildChanges(ZkUtils.BrokerTopicsPath, topicEventListener);

        // call to bootstrap topic list;
        topicEventListener.handleChildChange(ZkUtils.BrokerTopicsPath, topics);
    }

    private void stopWatchingTopicEvents() {
        zkClient.unsubscribeAll();
    }

    public void shutdown() {
        synchronized (lock) {
            info("Shutting down topic event watcher.");
            if (zkClient != null) {
                stopWatchingTopicEvents();
            } else {
                warn("Cannot shutdown since the embedded zookeeper client has already closed.");
            }
        }
    }

    class ZkTopicEventListener implements IZkChildListener {

        //    @throws(classOf<Exception>)
        public void handleChildChange(String parent, List<String> children) {
            synchronized (lock) {
                try {
                    if (zkClient != null) {
                        List<String> latestTopics = zkClient.getChildren(ZkUtils.BrokerTopicsPath);
                        debug(String.format("all topics: %s", latestTopics));
                        eventHandler.handleTopicEvent(latestTopics);
                    }
                } catch (Throwable e) {
                    error("error in handling child changes", e);
                }
            }
        }

    }

    class ZkSessionExpireListener implements IZkStateListener {
        ZkTopicEventListener topicEventListener;

        public ZkSessionExpireListener(ZkTopicEventListener topicEventListener) {
            this.topicEventListener = topicEventListener;
        }

        //        @throws(classOf<Exception>)
        public void handleStateChanged(Watcher.Event.KeeperState state) {
        }

        //        @throws(classOf<Exception>)
        public void handleNewSession() {
            synchronized (lock) {
                if (zkClient != null) {
                    info("ZK resubscribing expired topic event listener to topic registry");
                    zkClient.subscribeChildChanges(ZkUtils.BrokerTopicsPath, topicEventListener);
                }
            }
        }
    }
}
