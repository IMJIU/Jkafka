package kafka.utils;/**
 * Created by zhoulf on 2017/4/10.
 */

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author
 * @create 2017-04-10 49 16
 **/
public class SchedulerTest {

    KafkaScheduler scheduler = new KafkaScheduler(1);
    MockTime mockTime = new MockTime();
    AtomicInteger counter1 = new AtomicInteger(0);
    AtomicInteger counter2 = new AtomicInteger(0);

    @Before
    public void setup() {
        scheduler.startup();
    }

    @After
    public void teardown() {
        scheduler.shutdown();
    }

    @Test
    public void testMockSchedulerNonPeriodicTask() {
        mockTime.scheduler.schedule("test1", () -> counter1.getAndIncrement(), 1L);
        mockTime.scheduler.schedule("test2", () -> counter2.getAndIncrement(), 100L);
        Assert.assertEquals("Counter1 should not be incremented prior to task running.", 0, counter1.get());
        Assert.assertEquals("Counter2 should not be incremented prior to task running.", 0, counter2.get());
        mockTime.sleep(1L);
        Assert.assertEquals("Counter1 should be incremented", 1, counter1.get());
        Assert.assertEquals("Counter2 should not be incremented", 0, counter2.get());
        mockTime.sleep(100000L);
        Assert.assertEquals("More sleeping should not result in more incrementing on counter1.", 1, counter1.get());
        Assert.assertEquals("Counter2 should now be incremented.", 1, counter2.get());
    }

    @Test
    public void testMockSchedulerPeriodicTask() {
        mockTime.scheduler.schedule("test1", () -> counter1.getAndIncrement(), 1L, 1L, TimeUnit.MILLISECONDS);
        mockTime.scheduler.schedule("test2", () -> counter2.getAndIncrement(), 100L, 100L, TimeUnit.MILLISECONDS);
        Assert.assertEquals("Counter1 should not be incremented prior to task running.", 0, counter1.get());
        Assert.assertEquals("Counter2 should not be incremented prior to task running.", 0, counter2.get());
        mockTime.sleep(1L);
        Assert.assertEquals("Counter1 should be incremented", 1, counter1.get());
        Assert.assertEquals("Counter2 should not be incremented", 0, counter2.get());
        mockTime.sleep(100L);
        Assert.assertEquals("Counter1 should be incremented 101 times", 101, counter1.get());
        Assert.assertEquals("Counter2 should not be incremented once", 1, counter2.get());
    }

    @Test
    public void testReentrantTaskInMockScheduler() {
        mockTime.scheduler.schedule("test1", () -> mockTime.scheduler.schedule("test2", () -> counter2.getAndIncrement(), 0L), 1L);
        mockTime.sleep(1L);
        Assert.assertEquals(1, counter2.get());
    }

    @Test
    public void testNonPeriodicTask() throws InterruptedException {
        scheduler.schedule("test", () -> counter1.getAndIncrement(), 0L);
        TestUtils.retry(30000L, () -> Assert.assertEquals(counter1.get(), 1));
        Thread.sleep(5L);
        Assert.assertEquals("Should only run once", 1, counter1.get());
    }

    @Test
    public void testPeriodicTask() {
        scheduler.schedule("test", () -> counter1.getAndIncrement(), 0L, 5L, TimeUnit.MILLISECONDS);
        TestUtils.retry(30000L, () ->Assert.assertTrue("Should count to 20", counter1.get() >= 20));
    }
}
