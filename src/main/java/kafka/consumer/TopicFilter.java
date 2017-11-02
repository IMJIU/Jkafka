package kafka.consumer;

import kafka.common.Topic;
import kafka.utils.Logging;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * @author zhoulf
 * @create 2017-11-02 42 10
 **/


public abstract class TopicFilter extends Logging {
    public String rawRegex;

    public TopicFilter(String rawRegex) {
        this.rawRegex = rawRegex;
        regex = rawRegex
                .trim()
                .replace(',', '|')
                .replace(" ", "")
                .replaceAll("^<\" '>+", "")
                .replaceAll("<\" '>+$", ""); // property files may bring quotes;
        try {
            Pattern.compile(regex);
        } catch (PatternSyntaxException e) {
            throw new RuntimeException(regex + " is an invalid regex.");
        }
    }

    public String regex;


    @Override
    public String toString() {
        return regex;
    }

    public abstract boolean isTopicAllowed(String topic, Boolean excludeInternalTopics);
}

class Whitelist extends TopicFilter {
    public Whitelist(String rawRegex) {
        super(rawRegex);
    }

    @Override
    public boolean isTopicAllowed(String topic, Boolean excludeInternalTopics) {
        boolean allowed = topic.matches(regex) && !(Topic.InternalTopics.contains(topic) && excludeInternalTopics);
        debug(String.format("%s %s", topic, allowed ? "allowed" : "filtered"));
        return allowed;
    }
}

class Blacklist extends TopicFilter {
    public Blacklist(String rawRegex) {
        super(rawRegex);
    }

    @Override
    public boolean isTopicAllowed(String topic, Boolean excludeInternalTopics) {
        boolean allowed = (!topic.matches(regex)) && !(Topic.InternalTopics.contains(topic) && excludeInternalTopics);
        debug(String.format("%s %s", topic, (allowed) ? "allowed" : "filtered"));
        return allowed;
    }
}
