package unittest.com.weavelab.interview.ratelimit.strategy.impl;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/** Controllable monotonic clock and one-shot executor for deterministic timing tests. */
public final class ManualScheduler extends AbstractExecutorService
        implements ScheduledExecutorService {
    /** Monotonic nanosecond clock shared by the limiter and deterministic scheduling. */
    public final AtomicLong time = new AtomicLong();
    final List<Task> tasks = new CopyOnWriteArrayList<>();
    boolean stopped;

    /** Advances monotonic time and executes every task now due. */
    public void advance(long nanos) {
        time.addAndGet(nanos);
        runDue();
    }

    /** Executes due tasks, including tasks scheduled by another task during this run. */
    public void runDue() {
        while (true) {
            Task next =
                    tasks.stream()
                            .filter(t -> !t.isDone() && t.due <= time.get())
                            .min(Comparator.comparingLong(t -> t.due))
                            .orElse(null);
            if (next == null) return;
            next.run();
        }
    }

    /** Runs the next task before its deadline to exercise early-callback protection. */
    public void runEarly() {
        tasks.stream()
                .filter(t -> !t.isDone())
                .min(Comparator.comparingLong(t -> t.due))
                .orElseThrow()
                .run();
    }

    public ScheduledFuture<?> schedule(Runnable task, long delay, TimeUnit unit) {
        if (stopped) throw new RejectedExecutionException();
        Task result = new Task(task, time.get() + unit.toNanos(delay));
        tasks.add(result);
        return result;
    }

    final class Task extends FutureTask<Void> implements ScheduledFuture<Void> {
        final long due;

        Task(Runnable task, long due) {
            super(task, null);
            this.due = due;
        }

        public long getDelay(TimeUnit unit) {
            return unit.convert(due - time.get(), TimeUnit.NANOSECONDS);
        }

        public int compareTo(Delayed other) {
            return Long.compare(
                    getDelay(TimeUnit.NANOSECONDS), other.getDelay(TimeUnit.NANOSECONDS));
        }
    }

    public <V> ScheduledFuture<V> schedule(Callable<V> c, long d, TimeUnit u) {
        throw new UnsupportedOperationException();
    }

    public ScheduledFuture<?> scheduleAtFixedRate(Runnable r, long a, long b, TimeUnit u) {
        throw new UnsupportedOperationException();
    }

    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable r, long a, long b, TimeUnit u) {
        throw new UnsupportedOperationException();
    }

    public void shutdown() {
        stopped = true;
    }

    public List<Runnable> shutdownNow() {
        stopped = true;
        return List.of();
    }

    public boolean isShutdown() {
        return stopped;
    }

    public boolean isTerminated() {
        return stopped;
    }

    public boolean awaitTermination(long n, TimeUnit u) {
        return stopped;
    }

    public void execute(Runnable r) {
        schedule(r, 0, TimeUnit.NANOSECONDS);
    }
}
