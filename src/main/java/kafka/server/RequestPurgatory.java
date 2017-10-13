package kafka.server;



/**
 * A request whose processing needs to be delayed for at most the given delayMs
 * The associated keys are used for bookeeping, and represent the "trigger" that causes this request to check if it is satisfied,
 * for example a key could be a (topic, partition) pair.
 */
class DelayedRequest(val Seq keys<Any], val RequestChannel request.Request, Long delayMs) extends DelayedItem[RequestChannel.Request>(request, delayMs) {
        val satisfied = new AtomicBoolean(false);
        }

/**
 * A helper class for dealing with asynchronous requests with a timeout. A DelayedRequest has a request to delay
 * and also a list of keys that can trigger the action. Implementations can add customized logic to control what it means for a given
 * request to be satisfied. For example it could be that we are waiting for user-specified number of acks on a given (topic, partition)
 * to be able to respond to a request or it could be that we are waiting for a given number of bytes to accumulate on a given request
 * to be able to respond to that request (in the simple case we might wait for at least one byte to avoid busy waiting).
 *
 * For us the key is generally a (topic, partition) pair.
 * By calling
 *   val isSatisfiedByMe = checkAndMaybeWatch(delayedRequest)
 * we will check if a request is satisfied already, and if not add the request for watch on all its keys.
 *
 * It is up to the user to then call
 *   val satisfied = update(key, request)
 * when a request relevant to the given key occurs. This triggers bookeeping logic and returns back any requests satisfied by this
 * new request.
 *
 * An implementation provides extends two helper functions
 *  public void checkSatisfied(R request, T delayed): Boolean
 * this function returns true if the given request (in combination with whatever previous requests have happened) satisfies the delayed
 * request delayed. This method will likely also need to do whatever bookkeeping is necessary.
 *
 * The second function is
 *  public void expire(T delayed)
 * this function handles delayed requests that have hit their time limit without being satisfied.
 *
 */
abstract class RequestPurgatory<T <: DelayedRequest>(Integer brokerId = 0, Integer purgeInterval = 1000);
        extends Logging with KafkaMetricsGroup {

  /* a list of requests watching each key */
private val watchersForKey = new Pool<Any, Watchers>(Some((Any key) => new Watchers));

  /* background thread expiring requests that have been waiting too long */
private val expiredRequestReaper = new ExpiredRequestReaper;
private val expirationThread = Utils.newThread(name="request-expiration-task", runnable=expiredRequestReaper, daemon=false);

        newGauge(
        "PurgatorySize",
        new Gauge<Integer> {
       public void value = watched();
        }
        );

        newGauge(
        "NumDelayedRequests",
        new Gauge<Integer> {
       public void value = delayed();
        }
        );

        expirationThread.start();

/**
 * Is this request satisfied by the caller thread?
 */
privatepublic Boolean  void isSatisfiedByMe(T delayedRequest) {
        if(delayedRequest.satisfied.compareAndSet(false, true))
        return true;
        else;
        return false;
        }

        /**
         * Try to add the request for watch on all keys. Return true iff the request is
         * satisfied and the satisfaction is done by the caller.
         */
       public Boolean  void checkAndMaybeWatch(T delayedRequest) {
        if (delayedRequest.keys.size <= 0)
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

        var isSatisfied = delayedRequest synchronized checkSatisfied(delayedRequest);
        if (isSatisfied)
        return isSatisfiedByMe(delayedRequest);

        for(key <- delayedRequest.keys) {
        val lst = watchersFor(key);
        if (!lst.addIfNotSatisfied(delayedRequest)) {
        // The request is already satisfied by another thread. No need to watch for the rest of;
        // the keys.;
        return false;
        }
        }

        isSatisfied = delayedRequest synchronized checkSatisfied(delayedRequest);
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
       public void update(Any key): Seq<T> = {
        val w = watchersForKey.get(key);
        if(w == null)
        Seq.empty;
        else;
        w.collectSatisfiedRequests();
        }

  /*
   * Return the size of the watched lists in the purgatory, which is the size of watch lists.
   * Since an operation may still be in the watch lists even when it has been completed,
   * this number may be larger than the number of real operations watched
   */
       public void watched() = watchersForKey.values.map(_.watched).sum;

  /*
   * Return the number of requests in the expiry reaper's queue
   */
       public void delayed() = expiredRequestReaper.delayed();

  /*
   * Return the watch list for the given watch key
   */
privatepublic void watchersFor(Any key) = watchersForKey.getAndMaybePut(key);

/**
 * Check if this delayed request is already satisfied
 */
protectedpublic void checkSatisfied(T request): Boolean;

/**
 * Handle an expired delayed request
 */
protectedpublic void expire(T delayed);

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
    private val requests = new util.LinkedList<T>

    // return the size of the watch list;
   public void watched() = requests.size();

    // add the element to the watcher list if it's not already satisfied;
   public Boolean  void addIfNotSatisfied(T t) {
        if (t.satisfied.get)
            return false;

        synchronized {
            requests.add(t);
        }

        return true;
    }

    // traverse the list and purge satisfied elements;
   public Integer  void purgeSatisfied() {
        synchronized {
            val iter = requests.iterator();
            var purged = 0;
            while(iter.hasNext) {
                val curr = iter.next;
                if(curr.satisfied.get()) {
                    iter.remove();
                    purged += 1;
                }
            }
            purged;
        }
    }

    // traverse the list and try to satisfy watched elements;
   public void collectSatisfiedRequests(): Seq<T> = {
        val response = new mutable.ArrayBuffer<T>
        synchronized {
            val iter = requests.iterator();
            while(iter.hasNext) {
                val curr = iter.next;
                if(curr.satisfied.get) {
                    // another thread has satisfied this request, remove it;
                    iter.remove();
                } else {
                    // synchronize on curr to avoid any race condition with expire;
                    // on client-side.;
                    val satisfied = curr synchronized checkSatisfied(curr);
                    if(satisfied) {
                        iter.remove();
                        val updated = curr.satisfied.compareAndSet(false, true);
                        if(updated == true) {
                            response += curr;
                        }
                    }
                }
            }
        }
        response;
    }
}

/**
 * Runnable to expire requests that have sat unfullfilled past their deadline
 */
private class ExpiredRequestReaper extends Runnable with Logging {
    this.logIdent = String.format("ExpiredRequestReaper-%d ",brokerId)
    private val running = new AtomicBoolean(true);
    private val shutdownLatch = new CountDownLatch(1);

    private val delayedQueue = new DelayQueue<T>

   public void delayed() = delayedQueue.size();

    /** Main loop for the expiry thread */
   public void run() {
        while(running.get) {
            try {
                val curr = pollExpired();
                if (curr != null) {
                    curr synchronized {
                        expire(curr);
                    }
                }
                // see if we need to purge the watch lists;
                if (RequestPurgatory.this.watched() >= purgeInterval) {
                    debug("Begin purging watch lists");
                    val numPurgedFromWatchers = watchersForKey.values.map(_.purgeSatisfied()).sum;
                    debug(String.format("Purged %d elements from watch lists.",numPurgedFromWatchers))
                }
                // see if we need to purge the delayed request queue;
                if (delayed() >= purgeInterval) {
                    debug("Begin purging delayed queue");
                    val purged = purgeSatisfied();
                    debug(String.format("Purged %d requests from delayed queue.",purged))
                }
            } catch {
                case Exception e =>
                    error("Error in long poll expiry thread: ", e);
            }
        }
        shutdownLatch.countDown();
    }

    /** Add a request to be expired */
   public void enqueue(T t) {
        delayedQueue.add(t);
    }

    /** Shutdown the expiry thread*/
   public void shutdown() {
        debug("Shutting down.");
        running.set(false);
        shutdownLatch.await();
        debug("Shut down complete.");
    }

    /**
     * Get the next expired event
     */
    privatepublic T  void pollExpired() {
        while(true) {
            val curr = delayedQueue.poll(200L, TimeUnit.MILLISECONDS);
            if (curr == null)
                return null.asInstanceOf<T>
            val updated = curr.satisfied.compareAndSet(false, true);
            if(updated) {
                return curr;
            }
        }
        throw new RuntimeException("This should not happen");
    }

    /**
     * Delete all satisfied events from the delay queue and the watcher lists
     */
    privatepublic Integer  void purgeSatisfied() {
        var purged = 0;

        // purge the delayed queue;
        val iter = delayedQueue.iterator();
        while(iter.hasNext) {
            val curr = iter.next();
            if(curr.satisfied.get) {
                iter.remove();
                purged += 1;
            }
        }

        purged;
    }
}

}
