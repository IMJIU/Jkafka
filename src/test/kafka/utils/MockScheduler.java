package kafka.utils;

import kafka.func.Action;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;

/**
 * A mock scheduler that executes tasks synchronously using a mock time instance. Tasks are executed synchronously when
 * the time is advanced. This class is meant to be used in conjunction with MockTime.
 * <p>
 * Example usage
 * <code>
 * val time = new MockTime
 * time.scheduler.schedule("a task", println("hello world: " + time.milliseconds), delay = 1000)
 * time.sleep(1001) // this should cause our scheduled task to fire
 * </code>
 * <p>
 * Incrementing the time to the exact next execution time of a task will result in that task executing (it as if execution itself takes no time).
 */
public class MockScheduler extends KafkaScheduler {

    public Time time;
    public MockScheduler(Time time) {
        this.time = time;
    }

    /* a priority queue of tasks ordered by next execution time */
    public PriorityQueue<MockTask> tasks = new PriorityQueue<>();

    public void startup() {
    }

    public void shutdown() {
        synchronized (this) {
            tasks.clear();
        }
    }

    /**
     * Check for any tasks that need to execute. Since this is a mock scheduler this check only occurs
     * when this method is called and the execution happens synchronously in the calling thread.
     * If you are using the scheduler associated with a MockTime instance this call be triggered automatically.
     */
    public void tick() {
        synchronized (this) {
            Long now = time.milliseconds();
            while (!tasks.isEmpty() && tasks.peek().nextExecution <= now) {
        /* pop and execute the task with the lowest next execution time */
                MockTask curr = tasks.poll();
                curr.func.invoke();
        /* if the task is periodic, reschedule it and re-enqueue */
                if (curr.periodic()) {
                    curr.nextExecution += curr.period;
                    this.tasks.add(curr);
                }
            }
        }
    }

    public void schedule(String name, final Action action, Long delay) {
        schedule(name, action, delay, -1L, TimeUnit.MILLISECONDS);
    }
    public void schedule(String name, Action action,Long delay, Long period, TimeUnit unit ) {
        synchronized (this) {
            tasks.add(new MockTask(name, action, time.milliseconds() + delay, period));
            tick();
        }
    }

}


class MockTask implements Comparable<MockTask> {
    public String name;
    public Action func;
    public Long nextExecution;
    public Long period;

    public MockTask(String name, Action func, Long nextExecution, Long period) {
        this.name = name;
        this.func = func;
        this.nextExecution = nextExecution;
        this.period = period;
    }

    public boolean periodic() {
        return period >= 0;
    }

    @Override
    public int compareTo(MockTask b) {
        if (nextExecution == b.nextExecution)
            return 0;
        else if (nextExecution < b.nextExecution)
            return -1;
        else
            return 1;
    }
}
