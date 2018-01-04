package kafka.server;


import com.google.common.collect.Lists;
import com.yammer.metrics.core.Gauge;
import kafka.metrics.KafkaMetricsGroup;
import kafka.network.RequestChannel;
import kafka.utils.*;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A request whose processing needs to be delayed for at most the given delayMs
 * The associated keys are used for bookeeping, and represent the "trigger" that causes this request to check if it is satisfied,
 * for example a key could be a (topic, partition) pair.
 */
 class DelayedRequest extends DelayedItem<RequestChannel.Request> {
    public List  keys;
    public RequestChannel.Request request;
    public Long delayMs;
    public AtomicBoolean satisfied = new AtomicBoolean(false);

    public DelayedRequest(List keys, RequestChannel.Request request, Long delayMs) {
        super(request, delayMs);
        this.keys = keys;
        this.request = request;
        this.delayMs = delayMs;
    }
}

/**
 * A helper class for dealing with asynchronous requests with a timeout. A DelayedRequest has a request to delay
 * and also a list of keys that can trigger the action. Implementations can add customized logic to control what it means for a given
 * request to be satisfied. For example it could be that we are waiting for user-specified number of acks on a given (topic, partition)
 * to be able to respond to a request or it could be that we are waiting for a given number of bytes to accumulate on a given request
 * to be able to respond to that request (in the simple case we might wait for at least one byte to avoid busy waiting).
 * <p>
 * For us the key is generally a (topic, partition) pair.
 * By calling
 * val isSatisfiedByMe = checkAndMaybeWatch(delayedRequest)
 * we will check if a request is satisfied already, and if not add the request for watch on all its keys.
 * <p>
 * It is up to the user to then call
 * val satisfied = update(key, request)
 * when a request relevant to the given key occurs. This triggers bookeeping logic and returns back any requests satisfied by this
 * new request.
 * <p>
 * An implementation provides extends two helper functions
 * public void checkSatisfied(R request, T delayed): Boolean
 * this function returns true if the given request (in combination with whatever previous requests have happened) satisfies the delayed
 * request delayed. This method will likely also need to do whatever bookkeeping is necessary.
 * <p>
 * The second function is
 * public void expire(T delayed)
 * this function handles delayed requests that have hit their time limit without being satisfied.
 */
//abstract class RequestPurgatory<T <: DelayedRequest>(Integer brokerId = 0, Integer purgeInterval = 1000);
//        extends Logging with KafkaMetricsGroup {
abstract class RequestPurgatory<T extends DelayedRequest>
        extends KafkaMetricsGroup {
    public Integer brokerId;
    public Integer purgeInterval;

    public RequestPurgatory() {
        this(0, 100);
    }

    public RequestPurgatory(Integer brokerId, Integer purgeInterval) {
        this.brokerId = brokerId;
        this.purgeInterval = purgeInterval;
        init();
    }

    public void init() {
        newGauge("PurgatorySize", new Gauge<Object>() {
            public Object value() {
                return watched();
            }
        });

        newGauge("NumDelayedRequests", new Gauge<Integer>() {
            public Integer value() {
                return delayed();
            }
        });

        expirationThread.start();
    }

    /* a list of requests watching each key */
    private Pool<Object, Watchers> watchersForKey = new Pool<Object, Watchers>(Optional.of((key -> new Watchers())));

    /* background thread expiring requests that have been waiting too long */
    private ExpiredRequestReaper expiredRequestReaper = new ExpiredRequestReaper();
    private Thread expirationThread = Utils.newThread("request-expiration-task", expiredRequestReaper, false);


    /**
     * Is this request satisfied by the caller thread?
     */
    private Boolean isSatisfiedByMe(T delayedRequest) {
        if (delayedRequest.satisfied.compareAndSet(false, true))
            return true;
        else ;
        return false;
    }

    /**
     * Try to add the request for watch on all keys. Return true iff the request is
     * satisfied and the satisfaction is done by the caller.
     */
    public Boolean checkAndMaybeWatch(T delayedRequest) {
        if (delayedRequest.keys.size() <= 0)
            return isSatisfiedByMe(delayedRequest);

        // The cost of checkSatisfied() is typically proportional to the number of keys. Calling;
        // checkSatisfied() for each key is going to be expensive if there are many keys. Instead,
        // we do the check in the following way. Call checkSatisfied(). If the request is not satisfied,
        // we just add the request to all keys. Then we call checkSatisfied() again. At this time, if;
        // the request is still not satisfied, we are guaranteed that it won't miss any future triggering;
        // events since the request is already on the watcher list for all keys. This does mean that;
        // if the request is satisfied (by another thread) between the two checkSatisfied() calls, the;
        // request is unnecessarily added for watch. However, this is a less severe issue since the;
        // expire reaper will clean it up periodically.;
        boolean isSatisfied;
        synchronized (delayedRequest) {
            isSatisfied = checkSatisfied(delayedRequest);
        }
        if (isSatisfied)
            return isSatisfiedByMe(delayedRequest);

        for (Object key : delayedRequest.keys) {
            Watchers lst = watchersFor(key);
            if (!lst.addIfNotSatisfied(delayedRequest)) {
                // The request is already satisfied by another thread. No need to watch for the rest of;
                // the keys.;
                return false;
            }
        }
        synchronized (delayedRequest) {
            isSatisfied = checkSatisfied(delayedRequest);
        }
        if (isSatisfied)
            return isSatisfiedByMe(delayedRequest);
        else {
            // If the request is still not satisfied, add to the expire queue also.;
            expiredRequestReaper.enqueue(delayedRequest);
            return false;
        }
    }

    /**
     * Update any watchers and return a list of newly satisfied requests.
     */
    public List<T> update(Object key) {
        Watchers w = watchersForKey.get(key);
        if (w == null)
            return Collections.emptyList();
        else
            return w.collectSatisfiedRequests();
    }

    /*
     * Return the size of the watched lists in the purgatory, which is the size of watch lists.
     * Since an operation may still be in the watch lists even when it has been completed,
     * this number may be larger than the number of real operations watched
     */
    public Integer watched() {
        List<Integer> list = Sc.map(watchersForKey.values(), w -> w.watched());
        return Utils.sum(list).intValue();
    }

    /*
     * Return the number of requests in the expiry reaper's queue
     */
    public int delayed() {
        return expiredRequestReaper.delayed();
    }

    /*
     * Return the watch list for the given watch key
     */
    private Watchers watchersFor(Object key) {
        return watchersForKey.getAndMaybePut(key);
    }

    /**
     * Check if this delayed request is already satisfied
     */
    protected abstract Boolean checkSatisfied(T request);

    /**
     * Handle an expired delayed request
     */
    protected abstract void expire(T delayed);

    /**
     * Shutdown the expire reaper thread
     */
    public void shutdown() {
        expiredRequestReaper.shutdown();
    }

    /**
     * A linked list of DelayedRequests watching some key with some associated
     * bookkeeping logic.
     */
    private class Watchers {
        private List<T> requests = new LinkedList<>();

        // return the size of the watch list;
        public int watched() {
            return requests.size();
        }

        // add the element to the watcher list if it's not already satisfied;
        public Boolean addIfNotSatisfied(T t) {
            if (t.satisfied.get())
                return false;

            synchronized (this) {
                requests.add(t);
            }

            return true;
        }

        // traverse the list and purge satisfied elements;
        public Integer purgeSatisfied() {
            synchronized (this) {
                Iterator<T> iter = requests.iterator();
                int purged = 0;
                while (iter.hasNext()) {
                    T curr = iter.next();
                    if (curr.satisfied.get()) {
                        iter.remove();
                        purged += 1;
                    }
                }
                return purged;
            }
        }

        // traverse the list and try to satisfy watched elements;
        public List<T> collectSatisfiedRequests() {
            List<T> response = Lists.newArrayList();
            synchronized (this) {
                Iterator<T> iter = requests.iterator();
                while (iter.hasNext()) {
                    T curr = iter.next();
                    if (curr.satisfied.get()) {
                        // another thread has satisfied this request, remove it;
                        iter.remove();
                    } else {
                        // synchronize on curr to avoid any race condition with expire;
                        // on client-side.;
                        boolean satisfied;
                        synchronized (curr) {
                            satisfied = checkSatisfied(curr);
                        }
                        if (satisfied) {
                            iter.remove();
                            boolean updated = curr.satisfied.compareAndSet(false, true);
                            if (updated == true) {
                                response.add(curr);
                            }
                        }
                    }
                }
            }
            return response;
        }
    }

    /**
     * Runnable to expire requests that have sat unfullfilled past their deadline
     */
    private class ExpiredRequestReaper extends Logging implements Runnable {

        public String logIdent;
        private AtomicBoolean running = new AtomicBoolean(true);
        private CountDownLatch shutdownLatch = new CountDownLatch(1);

        private DelayQueue<T> delayedQueue = new DelayQueue<T>();

        public void init() {
            this.logIdent = String.format("ExpiredRequestReaper-%d ", brokerId);
        }

        public int delayed() {
            return delayedQueue.size();
        }

        /**
         * Main loop for the expiry thread
         */
        public void run() {
            while (running.get()) {
                try {
                    T curr = pollExpired();
                    if (curr != null) {
                        synchronized (curr) {
                            expire(curr);
                        }
                    }
                    // see if we need to purge the watch lists;
                    if (RequestPurgatory.this.watched() >= purgeInterval) {
                        debug("Begin purging watch lists");
                        int numPurgedFromWatchers = Utils.sum(Sc.map(watchersForKey.values(), w -> w.purgeSatisfied())).intValue();
                        debug(String.format("Purged %d elements from watch lists.", numPurgedFromWatchers));
                    }
                    // see if we need to purge the delayed request queue;
                    if (delayed() >= purgeInterval) {
                        debug("Begin purging delayed queue");
                        int purged = purgeSatisfied();
                        debug(String.format("Purged %d requests from delayed queue.", purged));
                    }
                } catch (Exception e) {
                    error("Error in long poll expiry thread: ", e);
                }
            }
            shutdownLatch.countDown();
        }

        /**
         * Add a request to be expired
         */
        public void enqueue(T t) {
            delayedQueue.add(t);
        }

        /**
         * Shutdown the expiry thread
         */
        public void shutdown() {
            debug("Shutting down.");
            running.set(false);
            try {
                shutdownLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            debug("Shut down complete.");
        }

        /**
         * Get the next expired event
         */
        private T pollExpired() {
            while (true) {
                T curr = null;
                try {
                    curr = delayedQueue.poll(200L, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (curr == null)
                    return null;
                boolean updated = curr.satisfied.compareAndSet(false, true);
                if (updated) {
                    return curr;
                }
            }
//            throw new RuntimeException("This should not happen");
        }

        /**
         * Delete all satisfied events from the delay queue and the watcher lists
         */
        private int purgeSatisfied() {
            int purged = 0;

            // purge the delayed queue;
            Iterator<T> iter = delayedQueue.iterator();
            while (iter.hasNext()) {
                T curr = iter.next();
                if (curr.satisfied.get()) {
                    iter.remove();
                    purged += 1;
                }
            }
            return purged;
        }
    }

}
