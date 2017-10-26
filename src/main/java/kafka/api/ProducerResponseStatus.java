package kafka.api;

/**
 * @author zhoulf
 * @create 2017-10-26 18:05
 **/
public class ProducerResponseStatus {
    public Short error;
    public Long offset;

    public ProducerResponseStatus(Short error, Long offset) {
        this.error = error;
        this.offset = offset;
    }
}

