package kafka.consumer;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import kafka.common.KafkaException;
import kafka.func.Tuple;
import kafka.utils.*;
import org.I0Itec.zkclient.*;

import java.util.*;

/**
 * @author zhoulf
 * @create 2017-11-01 59 17
 **/

public abstract class TopicCount extends Logging {
    public static Logging logger = Logging.getLogger(TopicCount.class.getName());

    public abstract Map<String, Set<ConsumerThreadId>> getConsumerThreadIdsPerTopic();

    public abstract Map<String, Integer> getTopicCountMap();

    public abstract String pattern();


    public static final String whiteListPattern = "white_list";
    public static final String blackListPattern = "black_list";
    public static final String staticPattern = "static";

    public String makeThreadId(String consumerIdString, Integer threadId) {
        return consumerIdString + "-" + threadId;
    }

    public static Map<String, Set<ConsumerThreadId>> makeConsumerThreadIdsPerTopic(String consumerIdString,
                                                                                   Map<String, Integer> topicCountMap) {
        Map<String, Set<ConsumerThreadId>> consumerThreadIdsPerTopicMap = Maps.newHashMap();
        topicCountMap.forEach((topic, nConsumers) -> {
            Set<ConsumerThreadId> consumerSet = Sets.newHashSet();
            assert (nConsumers >= 1);
            for (int i = 0; i < nConsumers; i++)
                consumerSet.add(new ConsumerThreadId(consumerIdString, i));
            consumerThreadIdsPerTopicMap.put(topic, consumerSet);
        });
        return consumerThreadIdsPerTopicMap;
    }

    public static TopicCount constructTopicCount(String group, String consumerId, ZkClient zkClient, Boolean excludeInternalTopics) {
        ZKGroupDirs dirs = new ZKGroupDirs(group);
        String topicCountString = ZkUtils.readData(zkClient, dirs.consumerRegistryDir() + "/" + consumerId).v1;
        String subscriptionPattern = null;
        Map<String, Integer> topMap = null;
        try {
            Map<String, Object> consumerRegistrationMap = JSON.parseObject(topicCountString, Map.class);
            if (consumerRegistrationMap != null) {
                Object pattern = consumerRegistrationMap.get("pattern");
                if (pattern != null) {
                    subscriptionPattern = pattern.toString();
                } else {
                    new KafkaException("error constructing TopicCount : " + topicCountString);
                }
                Object sub = consumerRegistrationMap.get("subscription");
                if (sub != null) {
                    topMap = (Map<String, Integer>) sub;
                } else {
                    throw new KafkaException("error constructing TopicCount : " + topicCountString);
                }
            } else {
                throw new KafkaException("error constructing TopicCount : " + topicCountString);
            }
        } catch (Throwable e) {
            logger.error("error parsing consumer json string " + topicCountString, e);
            throw e;
        }

        boolean hasWhiteList = whiteListPattern.equals(subscriptionPattern);
        boolean hasBlackList = blackListPattern.equals(subscriptionPattern);

        if (topMap.isEmpty() || !(hasWhiteList || hasBlackList)) {
            return new StaticTopicCount(consumerId, topMap);
        } else {
            Tuple<String, Integer> head = Sc.head(topMap);
            String regex = head.v1;
            Integer numStreams = head.v2;
            TopicFilter filter;
            if (hasWhiteList)
                filter = new Whitelist(regex);
            else
                filter = new Blacklist(regex);
            return new WildcardTopicCount(zkClient, consumerId, filter, numStreams, excludeInternalTopics);
        }
    }

    public TopicCount constructTopicCount(String consumerIdString, Map<String, Integer> topicCount) {
        return new StaticTopicCount(consumerIdString, topicCount);
    }


    public TopicCount constructTopicCount(String consumerIdString, TopicFilter filter, Integer numStreams, ZkClient zkClient, Boolean excludeInternalTopics) {
        return new WildcardTopicCount(zkClient, consumerIdString, filter, numStreams, excludeInternalTopics);
    }


}

class StaticTopicCount extends TopicCount {
    String consumerIdString;
    Map<String, Integer> topicCountMap;

    public StaticTopicCount(String consumerIdString, Map<String, Integer> topicCountMap) {
        this.consumerIdString = consumerIdString;
        this.topicCountMap = topicCountMap;
    }

    public Map<String, Set<ConsumerThreadId>> getConsumerThreadIdsPerTopic() {
        return TopicCount.makeConsumerThreadIdsPerTopic(consumerIdString, topicCountMap);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj != null && obj instanceof StaticTopicCount) {
            StaticTopicCount n = (StaticTopicCount) obj;
            return consumerIdString == n.consumerIdString && topicCountMap == n.topicCountMap;
        }
        return false;
    }

    public Map<String, Integer> getTopicCountMap() {
        return topicCountMap;
    }

    public String pattern() {
        return TopicCount.staticPattern;
    }
}


class WildcardTopicCount extends TopicCount {
    ZkClient zkClient;
    String consumerIdString;
    TopicFilter topicFilter;
    Integer numStreams;
    Boolean excludeInternalTopics;

    public WildcardTopicCount(ZkClient zkClient, String consumerIdString, TopicFilter topicFilter, Integer numStreams, Boolean excludeInternalTopics) {
        this.zkClient = zkClient;
        this.consumerIdString = consumerIdString;
        this.topicFilter = topicFilter;
        this.numStreams = numStreams;
        this.excludeInternalTopics = excludeInternalTopics;
    }

    public Map<String, Set<ConsumerThreadId>> getConsumerThreadIdsPerTopic() {
        List<String> wildcardTopics = Sc.filter(ZkUtils.getChildrenParentMayNotExist(zkClient, ZkUtils.BrokerTopicsPath),
                topic -> topicFilter.isTopicAllowed(topic, excludeInternalTopics));
        return TopicCount.makeConsumerThreadIdsPerTopic(consumerIdString,
                Sc.toMap(Sc.map(wildcardTopics, t -> Tuple.of(t, numStreams))));
    }

    public Map<String, Integer> getTopicCountMap() {
        return ImmutableMap.of(Utils.JSONEscapeString(topicFilter.regex), numStreams);
    }

    public String pattern() {
        if (topicFilter instanceof Whitelist) {
            return TopicCount.whiteListPattern;
        } else {
            return TopicCount.blackListPattern;
        }
    }

}
