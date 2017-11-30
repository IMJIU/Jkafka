package kafka.server;


import kafka.admin.AdminUtils;
import kafka.func.Tuple;
import kafka.log.Log;
import kafka.log.LogConfig;
import kafka.log.LogManager;
import kafka.log.TopicAndPartition;
import kafka.utils.Logging;
import kafka.utils.Sc;
import kafka.utils.Time;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.data.Stat;

import java.util.*;

/**
 * This class initiates and carries out topic config changes.
 * <p>
 * It works as follows.
 * <p>
 * Config is stored under the path
 * /brokers/topics/<topic_name>/config
 * This znode stores the topic- @Overrides for this topic (but no defaults) in properties format.
 * <p>
 * To avoid watching all topics for changes instead we have a notification path
 * /brokers/config_changes
 * The TopicConfigManager has a child watch on this path.
 * <p>
 * To update a topic config we first update the topic config properties. Then we create a new sequential
 * znode under the change path which contains the name of the topic that was updated, say
 * /brokers/config_changes/config_change_13321
 * This is just a notification--the actual config change is stored only once under the /brokers/topics/<topic_name>/config path.
 * <p>
 * This will fire a watcher on all brokers. This watcher works as follows. It reads all the config change notifications.
 * It keeps track of the highest config change suffix number it has applied previously. For any previously applied change it finds
 * it checks if this notification is larger than a static expiration time (say 10mins) and if so it deletes this notification.
 * For any new changes it reads the new configuration, combines it with the defaults, and updates the log config
 * for all logs for that topic (if any) that it has.
 * <p>
 * Note that config is always read from the config path in zk, the notification is just a trigger to do so. So if a broker is
 * down and misses a change that is fine--when it restarts it will be loading the full config anyway. Note also that
 * if there are two consecutive config changes it is possible that only the last one will be applied (since by the time the
 * broker reads the config the both changes may have been made). In this case the broker would needlessly refresh the config twice,
 * but that is harmless.
 * <p>
 * On restart the config manager re-processes all notifications. This will usually be wasted work, but avoids any race conditions
 * on startup where a change might be missed between the initial config load and registering for change notifications.
 */
class TopicConfigManager extends Logging {
    private ZkClient zkClient;
    private LogManager logManager;
    private Long changeExpirationMs = 15 * 60 * 1000L;
    private Time time = Time.get();
    private Long lastExecutedChange = -1L;

    public TopicConfigManager(ZkClient zkClient, LogManager logManager, java.lang.Long changeExpirationMs, Time time) {
        this.zkClient = zkClient;
        this.logManager = logManager;
        this.changeExpirationMs = changeExpirationMs;
        this.time = time;
    }

    public TopicConfigManager(ZkClient zkClient, LogManager logManager) {
        this.zkClient = zkClient;
        this.logManager = logManager;
        this.changeExpirationMs = 15 * 60 * 1000L;
        this.time = Time.get();
    }

    /**
     * Begin watching for config changes
     */
    public void startup() {
        ZkUtils.makeSurePersistentPathExists(zkClient, ZkUtils.TopicConfigChangesPath);
        zkClient.subscribeChildChanges(ZkUtils.TopicConfigChangesPath, new ConfigChangeListener());
        processAllConfigChanges();
    }

    /**
     * Process all config changes
     */
    private void processAllConfigChanges() {
        List<String> configChanges = zkClient.getChildren(ZkUtils.TopicConfigChangesPath);
        Collections.sort(configChanges);
        processConfigChanges(configChanges);
    }

    /**
     * Process the given list of config changes
     */
    private void processConfigChanges(List<String> notifications) {
        if (notifications.size() > 0) {
            info("Processing config change notification(s)...");
            Long now = time.milliseconds();
            Map<TopicAndPartition, Log> logs = logManager.logsByTopicPartition;
            Map<String, Map<TopicAndPartition, Log>> map = Sc.groupBy(logs, (k, v) -> k.topic);
            Map<String, List<Log>> logsByTopic = Sc.mapValue(map, v -> Sc.map(v, (kk, vv) -> vv));
            for (String notification : notifications) {
                Long changeId = changeNumber(notification);
                if (changeId > lastExecutedChange) {
                    String changeZnode = ZkUtils.TopicConfigChangesPath + "/" + notification;
                    Tuple<Optional<String>, Stat> tuple = ZkUtils.readDataMaybeNull(zkClient, changeZnode);
                    Optional<String> jsonOpt = tuple.v1;
                    Stat stat = tuple.v2;
                    if (jsonOpt.isPresent()) {
                        String json = jsonOpt.get();
                        String topic = json.substring(1, json.length() - 1); // hacky way to dequote;
                        if (logsByTopic.containsKey(topic)) {
              /* combine the default properties with the  @Overrides in zk to create the new LogConfig */
                            Properties props = new Properties(logManager.defaultConfig.toProps());
                            props.putAll(AdminUtils.fetchTopicConfig(zkClient, topic));
                            LogConfig logConfig = LogConfig.fromProps(props);
                            for (Log log : logsByTopic.get(topic))
                                log.config = logConfig;
                            info(String.format("Processed topic config change %d for topic %s, setting new config to %s.", changeId, topic, props));
                            purgeObsoleteNotifications(now, notifications);
                        }
                    }
                    lastExecutedChange = changeId;
                }
            }
        }
    }

    private void purgeObsoleteNotifications(Long now, List<String> notifications) {
        Collections.sort(notifications);
        for (String notification:notifications) {
            Tuple<Optional<String>, Stat> tuple  = ZkUtils.readDataMaybeNull(zkClient, ZkUtils.TopicConfigChangesPath + "/" + notification);
            Optional<String> jsonOpt = tuple.v1;
            Stat stat = tuple.v2;
            if (jsonOpt.isPresent()) {
                String changeZnode = ZkUtils.TopicConfigChangesPath + "/" + notification;
                if (now - stat.getCtime() > changeExpirationMs) {
                    debug("Purging config change notification " + notification);
                    ZkUtils.deletePath(zkClient, changeZnode);
                } else {
                    return;
                }
            }
        }
    }

    /* get the change number from a change notification znode */
    private Long changeNumber(String name) {
        return Long.parseLong(name.substring(AdminUtils.TopicConfigChangeZnodePrefix.length()));
    }

    /**
     * A listener that applies config changes to logs
     */
    class ConfigChangeListener implements IZkChildListener {
        @Override
        public void handleChildChange(String path, List<String> chillins) {
            try {
                processConfigChanges(chillins);
            } catch (Exception e) {
                error("Error processing config change:", e);
            }
        }
    }

}
