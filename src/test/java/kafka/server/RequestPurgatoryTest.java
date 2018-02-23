package kafka.server;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import kafka.api.*;
import kafka.common.*;
import kafka.log.TopicAndPartition;
import kafka.message.*;
import kafka.utils.*;
import org.junit.*;

import static org.junit.Assert.*;

import java.io.*;
import java.util.*;

public class RequestPurgatoryTest {

    ProducerRequest producerRequest1 = TestUtils.produceRequest("test", 0, new ByteBufferMessageSet(new Message("hello1".getBytes())));
    ProducerRequest producerRequest2 = TestUtils.produceRequest("test", 0, new ByteBufferMessageSet(new Message("hello2".getBytes())));
    MockRequestPurgatory purgatory = null;

    @Before
    public void setUp() {
        purgatory = new MockRequestPurgatory(5);
    }
//   @Override
//   public void tearDown() {
//    purgatory.shutdown();
//  }

    @Test
    public void testRequestSatisfaction() {
        DelayedRequest r1 = new DelayedRequest(Lists.newArrayList("test1"), null, 100000L);
        DelayedRequest r2 = new DelayedRequest(Lists.newArrayList("test2"), null, 100000L);
        Assert.assertEquals("With no waiting requests, nothing should be satisfied", 0, purgatory.update("test1").size());
        assertFalse("r1 not satisfied and hence watched", purgatory.checkAndMaybeWatch(r1));
        Assert.assertEquals("Still nothing satisfied", 0, purgatory.update("test1").size());
        assertFalse("r2 not satisfied and hence watched", purgatory.checkAndMaybeWatch(r2));
        Assert.assertEquals("Still nothing satisfied", 0, purgatory.update("test2").size());
        purgatory.satisfied.add(r1);
        Assert.assertEquals("r1 satisfied", Lists.newArrayList(r1), purgatory.update("test1"));
        Assert.assertEquals("Nothing satisfied", 0, purgatory.update("test1").size());
        purgatory.satisfied.add(r2);
        Assert.assertEquals("r2 satisfied", Lists.newArrayList(r2), purgatory.update("test2"));
        Assert.assertEquals("Nothing satisfied", 0, purgatory.update("test2").size());
    }

    @Test
    public void testRequestExpiry() {
        Long expiration = 20L;
        DelayedRequest r1 = new DelayedRequest(Lists.newArrayList("test1"), null, expiration);
        DelayedRequest r2 = new DelayedRequest(Lists.newArrayList("test1"), null, 200000L);
        Long start = System.currentTimeMillis();
        assertFalse("r1 not satisfied and hence watched", purgatory.checkAndMaybeWatch(r1));
        assertFalse("r2 not satisfied and hence watched", purgatory.checkAndMaybeWatch(r2));
        purgatory.awaitExpiration(r1);
        Long elapsed = System.currentTimeMillis() - start;
        Assert.assertTrue("r1 expired", purgatory.expired.contains(r1));
        Assert.assertTrue("r2 hasn't expired", !purgatory.expired.contains(r2));
        Assert.assertTrue(String.format("Time for expiration %d should at least %d", elapsed, expiration), elapsed >= expiration);
    }

    @Test
    public void testRequestPurge() {
        DelayedRequest r1 = new DelayedRequest(Lists.newArrayList("test1"), null, 100000L);
        DelayedRequest r12 = new DelayedRequest(Lists.newArrayList("test1", "test2"), null, 100000L);
        DelayedRequest r23 = new DelayedRequest(Lists.newArrayList("test2", "test3"), null, 100000L);
        purgatory.checkAndMaybeWatch(r1);
        purgatory.checkAndMaybeWatch(r12);
        purgatory.checkAndMaybeWatch(r23);

        Assert.assertEquals("Purgatory should have 5 watched elements", new Integer(5), purgatory.watched());
        Assert.assertEquals("Purgatory should have 3 total delayed requests", 3, purgatory.delayed());

        // satisfy one of the requests, it should then be purged from the watch list with purge interval 5;
        r12.satisfied.set(true);
        TestUtils.waitUntilTrue(() -> purgatory.watched() == 3,
                "Purgatory should have 3 watched elements instead of " + +purgatory.watched(), 1000L);
        TestUtils.waitUntilTrue(() -> purgatory.delayed() == 3,
                "Purgatory should still have 3 total delayed requests instead of " + purgatory.delayed(), 1000L);

        // add two more requests, then the satisfied request should be purged from the delayed queue with purge interval 5;
        purgatory.checkAndMaybeWatch(r1);
        purgatory.checkAndMaybeWatch(r1);

        TestUtils.waitUntilTrue(() -> purgatory.watched() == 5,
                "Purgatory should have 5 watched elements instead of " + purgatory.watched(), 1000L);
        TestUtils.waitUntilTrue(() -> purgatory.delayed() == 4,
                "Purgatory should have 4 total delayed requests instead of " + purgatory.delayed(), 1000L);
    }


    class MockRequestPurgatory extends RequestPurgatory<DelayedRequest> {
        Integer purge;

        public MockRequestPurgatory(Integer purge) {
            super(purge);
            this.purge = purge;
        }

        Set<DelayedRequest> satisfied = Sets.newHashSet();
        Set<DelayedRequest> expired = Sets.newHashSet();

        public void awaitExpiration(DelayedRequest delayed) {
            synchronized (delayed) {
                try {
                    delayed.wait();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        public Boolean checkSatisfied(DelayedRequest delayed) {
            return satisfied.contains(delayed);

        }

        public void expire(DelayedRequest delayed) {
            expired.add(delayed);
            synchronized (delayed) {
                delayed.notify();
            }
        }
    }

}
