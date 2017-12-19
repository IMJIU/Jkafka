package kafka.javaapi.consumer;

/**
 * @author zhoulf
 * @create 2017-12-19 15 20
 **/

import kafka.annotation.threadsafe;

/**
 * A consumer of kafka messages
 */
@threadsafe
class SimpleConsumer(val String host,
        val Int port,
        val Int soTimeout,
        val Int bufferSize,
        val String clientId) {

private val underlying = new kafka.consumer.SimpleConsumer(host, port, soTimeout, bufferSize, clientId);

        /**
         *  Fetch a set of messages from a topic. This version of the fetch method
         *  takes the Scala version of a fetch request (i.e.,
         *  <<kafka.api.FetchRequest>> and is intended for use with the
         *  <<kafka.api.FetchRequestBuilder>>.
         *
         *  @param request  specifies the topic name, topic partition, starting byte offset, maximum bytes to be fetched.
         *  @return a set of fetched messages
         */
       public FetchResponse  void fetch(kafka request.api.FetchRequest) {
        import kafka.javaapi.Implicits._;
        underlying.fetch(request);
        }

        /**
         *  Fetch a set of messages from a topic.
         *
         *  @param request specifies the topic name, topic partition, starting byte offset, maximum bytes to be fetched.
         *  @return a set of fetched messages
         */
       public FetchResponse  void fetch(kafka request.javaapi.FetchRequest) {
        fetch(request.underlying);
        }

        /**
         *  Fetch metadata for a sequence of topics.
         *
         *  @param request specifies the versionId, clientId, sequence of topics.
         *  @return metadata for each topic in the request.
         */
       public void send(kafka request.javaapi.TopicMetadataRequest): kafka.javaapi.TopicMetadataResponse = {
        import kafka.javaapi.Implicits._;
        underlying.send(request.underlying);
        }

        /**
         *  Get a list of valid offsets (up to maxSize) before the given time.
         *
         *  @param request a <<kafka.javaapi.OffsetRequest>> object.
         *  @return a <<kafka.javaapi.OffsetResponse>> object.
         */
       public void getOffsetsBefore(OffsetRequest request): kafka.javaapi.OffsetResponse = {
        import kafka.javaapi.Implicits._;
        underlying.getOffsetsBefore(request.underlying)
        }

        /**
         * Commit offsets for a topic to Zookeeper
         * @param request a <<kafka.javaapi.OffsetCommitRequest>> object.
         * @return a <<kafka.javaapi.OffsetCommitResponse>> object.
         */
       public void commitOffsets(kafka request.javaapi.OffsetCommitRequest): kafka.javaapi.OffsetCommitResponse = {
        import kafka.javaapi.Implicits._;
        underlying.commitOffsets(request.underlying);
        }

        /**
         * Fetch offsets for a topic from Zookeeper
         * @param request a <<kafka.javaapi.OffsetFetchRequest>> object.
         * @return a <<kafka.javaapi.OffsetFetchResponse>> object.
         */
       public void fetchOffsets(kafka request.javaapi.OffsetFetchRequest): kafka.javaapi.OffsetFetchResponse = {
        import kafka.javaapi.Implicits._;
        underlying.fetchOffsets(request.underlying);
        }

       public void close() {
        underlying.close;
        }
        }
