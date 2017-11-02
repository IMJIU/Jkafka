package kafka.common;

import com.google.common.collect.Sets;
import kafka.server.OffsetManager;
import org.apache.kafka.common.errors.InvalidTopicException;

import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author zhoulf
 * @create 2017-11-02 11:05
 **/


public class Topic {
    public static final String legalChars = "[a-zA-Z0-9\\._\\-]";
    private static final Integer maxNameLength = 255;
    //    private Regex rgx = new Regex(legalChars + "+");
    private Pattern pattern = Pattern.compile("(" + legalChars + "+)");
    public static final Set<String> InternalTopics = Sets.newHashSet(OffsetManager.OffsetsTopicName);

    public void validate(String topic) {
        if (topic.length() <= 0)
            throw new InvalidTopicException("topic name is illegal, can't be empty");
        else if (topic.equals(".") || topic.equals(".."))
            throw new InvalidTopicException("topic name cannot be \".\" or \"..\"");
        else if (topic.length() > maxNameLength)
            throw new InvalidTopicException("topic name is illegal, can't be longer than " + maxNameLength + " characters");
        Matcher match = pattern.matcher(topic);
        if (match.matches()) {
            String t = match.group(1);
            if (t != topic) {
                throw new InvalidTopicException("topic name " + topic + " is illegal, contains a character other than ASCII alphanumerics, '.', '_' and '-'");
            }
        } else {
            throw new InvalidTopicException("topic name " + topic + " is illegal,  contains a character other than ASCII alphanumerics, '.', '_' and '-'");
        }
    }
}
