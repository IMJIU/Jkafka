package kafka.consumer;

import kafka.utils.Logging;
import org.I0Itec.zkclient.ZkClient;

import java.util.Map;
import java.util.Set;

/**
 * @author zhoulf
 * @create 2017-11-01 59 17
 **/

public abstract class TopicCount  extends Logging{

   public abstract Map<String, Set<ConsumerThreadId>> getConsumerThreadIdsPerTopic();

    public  abstract Map<String, Integer> getTopicCountMap();

    public abstract String pattern();


        String whiteListPattern="white_list";
    String blackListPattern="black_list";
    String staticPattern="static";

public String makeThreadId(String consumerIdString,Integer threadId){
    return consumerIdString+"-"+threadId;
}

public void makeConsumerThreadIdsPerTopic(String consumerIdString,
        Map<String, Integer> topicCountMap){
        val consumerThreadIdsPerTopicMap=new mutable.HashMap<String, Set<ConsumerThreadId>>();
        for((topic,nConsumers)<-topicCountMap){
        val consumerSet=new mutable.HashSet<ConsumerThreadId>
        assert(nConsumers>=1);
                for(i<-0until nConsumers)
        consumerSet+=ConsumerThreadId(consumerIdString,i);
        consumerThreadIdsPerTopicMap.put(topic,consumerSet);
        }
        consumerThreadIdsPerTopicMap;
        }

public static TopicCount  constructTopicCount(String group, String consumerId, ZkClient zkClient, Boolean excludeInternalTopics){
    ZKGroupDirs dirs=new ZKGroupDirs(group);
        val topicCountString=ZkUtils.readData(zkClient,dirs.consumerRegistryDir+"/"+consumerId)._1;
        var String subscriptionPattern=null;
        var Map topMap<String, Integer> =null;
        try{
        Json.parseFull(topicCountString)match{
        case Some(m)=>
        val consumerRegistrationMap=m.asInstanceOf<Map<String, Any>>
        consumerRegistrationMap.get("pattern")match{
        case Some(pattern)=>subscriptionPattern=pattern.asInstanceOf<String>
        case None=>throw new KafkaException("error constructing TopicCount : "+topicCountString);
                }
                consumerRegistrationMap.get("subscription")match{
                case Some(sub)=>topMap=sub.asInstanceOf<Map<String, Int>>
        case None=>throw new KafkaException("error constructing TopicCount : "+topicCountString);
        }
        case None=>throw new KafkaException("error constructing TopicCount : "+topicCountString);
        }
        }catch{
        case Throwable e=>
        error("error parsing consumer json string "+topicCountString,e);
        throw e;
        }

        val hasWhiteList=whiteListPattern.equals(subscriptionPattern);
        val hasBlackList=blackListPattern.equals(subscriptionPattern);

        if(topMap.isEmpty||!(hasWhiteList||hasBlackList)){
        new StaticTopicCount(consumerId,topMap);
        }else{
        val regex=topMap.head._1;
        val numStreams=topMap.head._2;
        val filter=
        if(hasWhiteList)
        new Whitelist(regex);
        else;
        new Blacklist(regex);
        new WildcardTopicCount(zkClient,consumerId,filter,numStreams,excludeInternalTopics);
        }
        }

public void constructTopicCount(String consumerIdString,Map topicCount<String, Integer>)=
        new StaticTopicCount(consumerIdString,topicCount);

public void constructTopicCount(String consumerIdString,TopicFilter filter,Int numStreams,ZkClient zkClient,Boolean excludeInternalTopics)=
        new WildcardTopicCount(zkClient,consumerIdString,filter,numStreams,excludeInternalTopics);

        }

class StaticTopicCount extends TopicCount{
    String consumerIdString;
     Map <String, Integer>topicCountMap;

    public StaticTopicCount(String consumerIdString, Map<String, Integer> topicCountMap) {
        this.consumerIdString = consumerIdString;
        this.topicCountMap = topicCountMap;
    }

    public void getConsumerThreadIdsPerTopic(){
        return TopicCount.makeConsumerThreadIdsPerTopic(consumerIdString,topicCountMap);
    }

@Override
public Boolean  equals(Object obj){
        obj match{
        case null=>false;
        case StaticTopicCount n=>consumerIdString==n.consumerIdString&&topicCountMap==n.topicCountMap;
        case _=>false;
        }
        }

public void getTopicCountMap=topicCountMap;

public void pattern=TopicCount.staticPattern;
        }


class WildcardTopicCount extends TopicCount{
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

    public void getConsumerThreadIdsPerTopic={
        val wildcardTopics=ZkUtils.getChildrenParentMayNotExist(zkClient,ZkUtils.BrokerTopicsPath);
        .filter(topic=>topicFilter.isTopicAllowed(topic,excludeInternalTopics));
        TopicCount.makeConsumerThreadIdsPerTopic(consumerIdString,Map(wildcardTopics.map((_,numStreams)):_*));
        }

public void getTopicCountMap=Map(Utils.JSONEscapeString(topicFilter.regex)->numStreams);

public void String pattern={
        topicFilter match{
        case Whitelist wl=>TopicCount.whiteListPattern;
        case Blacklist bl=>TopicCount.blackListPattern;
        }
        }

        }
