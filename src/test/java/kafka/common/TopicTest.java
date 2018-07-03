package kafka.common;

import com.google.common.collect.Lists;
import kafka.utils.Logging;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.junit.Test;

import java.util.List;

/**
 * @author zhoulf
 * @create 2017-12-13 55 17
 **/

public class TopicTest extends Logging {

    @Test
    public void testInvalidTopicNames() {
        List<String> invalidTopicNames = Lists.newArrayList();
        invalidTopicNames.addAll(Lists.newArrayList("", ".", ".."));
        String longName = "ATCG";
        for (int i = 1; i <= 6; i++)
            longName += longName;
        invalidTopicNames.add(longName);
        List<Character> badChars = Lists.newArrayList('/', '\\', ',', '\u0000', ':', '\"', '\'', ';', '*', '?', ' ', '\t', '\r', '\n', '=');
        for (Character weirdChar : badChars) {
            invalidTopicNames.add("Is" + weirdChar + "illegal");
        }

        for (int i = 0; i < invalidTopicNames.size(); i++) {
            try {
                Topic.validate(invalidTopicNames.get(i));
                error("Should throw InvalidTopicException.[" + invalidTopicNames.get(i) + "]");
            } catch (InvalidTopicException e) {
                info("This is good.");
            }
        }

        List<String> validTopicNames = Lists.newArrayList();
        validTopicNames.addAll(Lists.newArrayList("valid", "TOPIC", "nAmEs", "ar6", "VaL1d", "_0-9_."));
        for (int i = 0; i < validTopicNames.size(); i++) {
            try {
                Topic.validate(validTopicNames.get(i));
            } catch (Throwable e) {
                error("Should not throw exception.");
            }
        }
    }
}
