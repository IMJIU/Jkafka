package kafka.utils;

/**
 * @author zhoulf
 * @create 2017-11-02 10:06
 **/
public class ZKGroupDirs {
    public String group;

    public ZKGroupDirs(String group) {
        this.group = group;
    }

    public String consumerDir() {
        return ZkUtils.ConsumersPath;
    }

    public String consumerGroupDir() {
        return consumerDir() + "/" + group;
    }

    public String consumerRegistryDir() {
        return consumerGroupDir() + "/ids";
    }
}
