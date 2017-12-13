package kafka.common;

import com.google.common.collect.Lists;
import kafka.consumer.ConsumerConfig;
import kafka.producer.ProducerConfig;
import kafka.utils.Logging;
import org.junit.Test;

import java.util.List;

public class ConfigTest extends Logging {

    @Test
    public void testInvalidClientIds() {
        List<String> invalidClientIds = Lists.newArrayList();
        List<Character> badChars = Lists.newArrayList('/', '\\', ',', '\u0000', ':', '\"', '\'', ';', '*', '?', ' ', '\t', '\r', '\n', '=');
        for (Character weirdChar : badChars) {
            invalidClientIds.add("Is" + weirdChar + "illegal");
        }

        for (int i = 0; i < invalidClientIds.size(); i++) {
            try {
                ProducerConfig.validateClientId(invalidClientIds.get(i));
                error("Should throw InvalidClientIdException.");
            } catch (InvalidConfigException e) {
                info("This is good.");
            }
        }

        List<String> validClientIds = Lists.newArrayList();
        validClientIds.addAll(Lists.newArrayList("valid", "CLIENT", "iDs", "ar6", "VaL1d", "_0-9_.", ""));
        for (int i = 0; i < validClientIds.size(); i++) {
            try {
                ProducerConfig.validateClientId(validClientIds.get(i));
            } catch (Exception e) {
                error("Should not throw exception.");
            }
        }
    }

    @Test
    public void testInvalidGroupIds() {
        List<String> invalidGroupIds = Lists.newArrayList();
        List<Character> badChars = Lists.newArrayList('/', '\\', ',', '\u0000', ':', '\"', '\'', ';', '*', '?', ' ', '\t', '\r', '\n', '=');
        for (Character weirdChar : badChars) {
            invalidGroupIds.add("Is" + weirdChar + "illegal");
        }
        for (int i = 0; i < invalidGroupIds.size(); i++) {
            try {
                ConsumerConfig.validateGroupId(invalidGroupIds.get(i));
                error("Should throw InvalidGroupIdException.");
            } catch (InvalidConfigException e) {
                info("This is good.");
            }
        }
        List<String> validGroupIds = Lists.newArrayList();
        validGroupIds.addAll(Lists.newArrayList("valid", "GROUP", "iDs", "ar6", "VaL1d", "_0-9_.", ""));
        for (int i = 0; i < validGroupIds.size(); i++) {
            try {
                ConsumerConfig.validateGroupId(validGroupIds.get(i));
            } catch (Exception e) {
                error("Should not throw exception.");
            }
        }
    }
}
