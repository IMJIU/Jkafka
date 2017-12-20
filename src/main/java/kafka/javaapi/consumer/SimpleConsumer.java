package kafka.javaapi.consumer;

/**
 * @author zhoulf
 * @create 2017-12-19 15 20
 **/

import kafka.annotation.threadsafe;
import kafka.api.FetchResponse;

/**
 * A consumer of kafka messages
 */
@threadsafe
public class SimpleConsumer {
    public String host;
    public Integer port;
    public Integer soTimeout;
    public Integer bufferSize;
    public String clientId;
    private kafka.consumer.SimpleConsumer underlying = new kafka.consumer.SimpleConsumer(host, port, soTimeout, bufferSize, clientId);

    /**
     * Fetch a set of messages from a topic. This version of the fetch method
     * takes the Scala version of a fetch request (i.e.,
     * <<kafka.api.FetchRequest>> and is intended for use with the
     * <<kafka.api.FetchRequestBuilder>>.
     *
     * @param request specifies the topic name, topic partition, starting byte offset, maximum bytes to be fetched.
     * @return a set of fetched messages
     */
    public FetchResponse fetch(kafka.api.FetchRequest request) {
        return underlying.fetch(request);
    }

    /**
     * Fetch a set of messages from a topic.
     *
     * @param request specifies the topic name, topic partition, starting byte offset, maximum bytes to be fetched.
     * @return a set of fetched messages
     */
    public FetchResponse fetch(kafka.javaapi.FetchRequest request) {
        return fetch(request.underlying);
    }

    /**
     * Fetch metadata for a sequence of topics.
     *
     * @param request specifies the versionId, clientId, sequence of topics.
     * @return metadata for each topic in the request.
     */
    public kafka.javaapi.TopicMetadataResponse send(kafka.javaapi.TopicMetadataRequest request) {
        return underlying.send(request.underlying);
    }

    /**
     * Get a list of valid offsets (up to maxSize) before the given time.
     *
     * @param request a <<kafka.javaapi.OffsetRequest>> object.
     * @return a <<kafka.javaapi.OffsetResponse>> object.
     */
    public kafka.javaapi.OffsetResponse getOffsetsBefore(OffsetRequest request) {
        return underlying.getOffsetsBefore(request.underlying)
    }

    /**
     * Commit offsets for a topic to Zookeeper
     *
     * @param request a <<kafka.javaapi.OffsetCommitRequest>> object.
     * @return a <<kafka.javaapi.OffsetCommitResponse>> object.
     */
    public kafka.javaapi.OffsetCommitResponse commitOffsets(kafka.javaapi.OffsetCommitRequest request) {
        return underlying.commitOffsets(request.underlying);
    }

    /**
     * Fetch offsets for a topic from Zookeeper
     *
     * @param request a <<kafka.javaapi.OffsetFetchRequest>> object.
     * @return a <<kafka.javaapi.OffsetFetchResponse>> object.
     */
    public kafka.javaapi.OffsetFetchResponse fetchOffsets(kafka.javaapi.OffsetFetchRequest request) {
        return underlying.fetchOffsets(request.underlying);
    }

    public void close() {
        underlying.close();
    }
}
