package com.weavelab.interview.ratelimit.service.impl;

import static com.weavelab.interview.ratelimit.service.model.RateLimitDecision.Outcome.*;

import com.weavelab.interview.ratelimit.service.IQueuedRateLimiter;
import com.weavelab.interview.ratelimit.service.ISubmission;
import com.weavelab.interview.ratelimit.service.model.RateLimitDecision;
import com.weavelab.interview.ratelimit.service.model.RateLimitDecisionEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * Process-local token bucket with a bounded FIFO waiting queue.
 *
 * <p>The bucket starts with {@code capacity} tokens and earns one token per interval, up to
 * capacity. Each admission consumes one token. A request takes an available token immediately if no
 * older request is waiting; otherwise it queues until a token becomes available or its
 * enqueue-relative deadline expires. Capacity independently bounds both stored tokens and waiting
 * entries. Already released work counts toward neither queue size.
 *
 * <p>Refill is calculated from elapsed monotonic time, retaining fractional intervals when the
 * bucket is not full. Idle credit cannot exceed capacity. Delayed callbacks can release multiple
 * FIFO requests using accumulated tokens, but cannot create more than capacity tokens. Timeout wins
 * when expiration and token availability coincide; expired entries consume no token.
 *
 * <p>State and snapshots share one lock. Observer callbacks and future completion run outside it,
 * with per-entry ordered delivery preserving ENQUEUED before its terminal event. The externally
 * owned scheduler wakes only while requests wait, at the next token or earliest expiration. Servlet
 * suspension and dispatch belong to the HTTP adapter.
 */
public final class BoundedQueueRateLimiter implements IQueuedRateLimiter {
    private static final Logger LOG = LoggerFactory.getLogger(BoundedQueueRateLimiter.class);

    // Maximum stored tokens and maximum waiting entries (two independent bounds).
    private final int capacity;
    private final long interval, timeout;
    private final LongSupplier ticker;
    private final ScheduledExecutorService scheduler;
    private final Deque<Entry> queue = new ArrayDeque<>();
    // Mutable scheduling/queue state below is guarded by this limiter's monitor.
    private ScheduledFuture<?> task;
    private int tokens;
    private long lastRefill;
    private boolean closed;

    /**
     * Creates a full token bucket and empty queue; no task runs until a request waits.
     *
     * @param capacity positive token-bucket size and maximum number of waiting requests
     * @param interval positive time required to earn one token
     * @param timeout positive maximum time a request may remain queued
     * @param ticker monotonic nanosecond source, normally {@code System::nanoTime}
     * @param scheduler externally owned executor; this limiter cancels its tasks but does not shut
     *     the executor down
     */
    public BoundedQueueRateLimiter(
            int capacity,
            Duration interval,
            Duration timeout,
            LongSupplier ticker,
            ScheduledExecutorService scheduler) {
        LOG.info("event=method_invocation method=BoundedQueueRateLimiter.<init>");
        this.interval = LimiterSupport.interval(capacity, interval);
        this.capacity = capacity;
        this.timeout = timeout.toNanos();
        if (this.timeout <= 0) throw new IllegalArgumentException("Timeout must be positive");
        this.ticker = Objects.requireNonNull(ticker);
        this.scheduler = Objects.requireNonNull(scheduler);
        this.tokens = capacity;
        this.lastRefill = ticker.getAsLong();
    }

    /**
     * Returns a handle that may already contain ALLOWED, RATE_LIMITED or STOPPED, or may remain
     * pending until release, timeout, cancellation or shutdown. No thread waits here for the
     * release interval. Observer runtime exceptions do not change admission.
     *
     * <p>Waiting entries emit ENQUEUED followed by one terminal event. Immediate decisions emit
     * only their terminal event. Retry-after for a full queue is advisory and reflects the next
     * token deadline, not a reservation for the rejected caller.
     */
    public ISubmission submit(Consumer<RateLimitDecisionEvent> observer) {
        LOG.info("event=method_invocation method=BoundedQueueRateLimiter.submit");
        List<Runnable> notifications = new ArrayList<>();
        Entry entry;
        synchronized (this) {
            long now = ticker.getAsLong();
            int before = queue.size();
            entry = new Entry(now, observer);
            if (closed) {
                finish(entry, STOPPED, "LIMITER_CLOSED", now, before, notifications);
            } else {
                processWaiting(now, notifications);
                before = queue.size();
                if (queue.isEmpty() && tokens > 0) {
                    tokens--;
                    finish(entry, ALLOWED, "TOKEN_AVAILABLE", now, before, notifications);
                } else if (queue.size() == capacity) {
                    finish(entry, RATE_LIMITED, "QUEUE_FULL", now, before, notifications);
                } else {
                    queue.add(entry);
                    var event = event(entry, "ENQUEUED", "WAITING_FOR_TOKEN", now, before, 0);
                    entry.deliveries.add(() -> LimiterSupport.emit(observer, event));
                    notifications.add(entry::deliver);
                }
                reschedule(now, notifications);
            }
        }
        notifications.forEach(Runnable::run);

        return entry;
    }

    /**
     * Replaces the wake-up with the earliest token availability or expiration. Caller holds the
     * lock.
     */
    private void reschedule(long now, List<Runnable> notifications) {
        if (task != null) {
            task.cancel(false);
            task = null;
        }
        if (queue.isEmpty() || closed)
            return;
        long delay = nextTokenDelay(now);
        for (Entry e : queue)
            delay = Math.min(delay, Math.max(0, timeout - (now - e.enqueued)));
        try {
            task = scheduler.schedule(this::tick, delay, TimeUnit.NANOSECONDS);
        } catch (RejectedExecutionException failure) {
            notifications.add(
                    () ->
                            LOG.error(
                                    "Rate limit scheduler rejected pending work; stopping queue",
                                    failure));
            // Without a scheduler, pending requests cannot progress; settle them as STOPPED.
            stop(now, notifications);
        }
    }

    /** Expires requests before spending tokens, preserving FIFO even on late callbacks. */
    private void processWaiting(long now, List<Runnable> notifications) {
        refill(now);
        for (Iterator<Entry> it = queue.iterator(); it.hasNext(); ) {
            Entry e = it.next();
            if (now - e.enqueued >= timeout) {
                int before = queue.size();
                it.remove();
                finish(e, TIMED_OUT, "QUEUE_TIMEOUT", now, before, notifications);
            }
        }
        while (!queue.isEmpty() && tokens > 0) {
            int before = queue.size();
            Entry e = queue.removeFirst();
            tokens--;
            finish(e, ALLOWED, "TOKEN_AVAILABLE", now, before, notifications);
        }
    }

    /** Retains partial refill time; discards credit beyond a full bucket. Caller holds the lock. */
    private void refill(long now) {
        if (tokens == capacity) {
            lastRefill = now;
            return;
        }
        long earned = Math.max(0, now - lastRefill) / interval;
        if (earned == 0)
            return;
        if (earned >= capacity - tokens) {
            tokens = capacity;
            lastRefill = now;
        } else {
            tokens += (int) earned;
            lastRefill += earned * interval;
        }
    }

    private long nextTokenDelay(long now) {
        return tokens > 0 ? 0 : Math.max(0, interval - Math.max(0, now - lastRefill));
    }

    private void tick() {
        List<Runnable> notifications = new ArrayList<>();
        synchronized (this) {
            if (closed)
                return;
            long now = ticker.getAsLong();
            processWaiting(now, notifications);
            reschedule(now, notifications);
        }
        notifications.forEach(Runnable::run);
    }

    /** Captures state under the lock; later logging never reads a changing queue. */
    private RateLimitDecisionEvent event(
            Entry e, String decision, String reason, long now, int before, long retry) {
        return new RateLimitDecisionEvent(
                decision,
                reason,
                Duration.ofNanos(retry),
                Duration.ofNanos(Math.max(0, now - e.enqueued)),
                Map.of(
                        "queueSizeBefore",
                        (long) before,
                        "queueSizeAfter",
                        (long) queue.size(),
                        "waitingCapacity",
                        (long) capacity,
                        "availableTokens",
                        (long) tokens,
                        "tokenCapacity",
                        (long) capacity,
                        "nextTokenInNs",
                        nextTokenDelay(now),
                        "deadlineRemainingNs",
                        Math.max(0, timeout - (now - e.enqueued))));
    }

    /**
     * Marks a terminal transition under the lock and defers its notification/completion. Callers
     * must ensure the entry has not already reached a terminal state.
     */
    private void finish(
            Entry e,
            RateLimitDecision.Outcome outcome,
            String reason,
            long now,
            int before,
            List<Runnable> notifications) {
        e.terminal = true;
        long retry = outcome == RATE_LIMITED ? nextTokenDelay(now) : 0;
        var event = event(e, outcome.name(), reason, now, before, retry);
        var result = new RateLimitDecision(outcome, Duration.ofNanos(retry));
        e.deliveries.add(
                () -> {
                    LimiterSupport.emit(e.observer, event);
                    e.future.complete(result);
                });
        notifications.add(e::deliver);
    }

    /** Stops future admission and collects terminal notifications for all waiting entries. */
    private void stop(long now, List<Runnable> notifications) {
        closed = true;
        if (task != null) {
            task.cancel(false);
            task = null;
        }
        while (!queue.isEmpty()) {
            int before = queue.size();
            finish(queue.removeFirst(), STOPPED, "LIMITER_CLOSED", now, before, notifications);
        }
    }

    /**
     * Permanently stops admission, cancels the scheduled task and settles pending entries as
     * STOPPED. Already released work is unaffected. Does not shut down the shared executor.
     */
    public void close() {
        LOG.info("event=method_invocation method=BoundedQueueRateLimiter.close");
        List<Runnable> notifications = new ArrayList<>();
        synchronized (this) {
            stop(ticker.getAsLong(), notifications);
        }
        notifications.forEach(Runnable::run);
    }

    /** One request's enqueue time, completion handle and ordered notification mailbox. */
    private final class Entry implements ISubmission {
        final long enqueued;
        final Consumer<RateLimitDecisionEvent> observer;
        final CompletableFuture<RateLimitDecision> future = new CompletableFuture<>();
        // Publication occurs under the limiter lock; delivery can run concurrently outside it.
        final Queue<Runnable> deliveries = new ConcurrentLinkedQueue<>();
        final AtomicBoolean delivering = new AtomicBoolean();
        // Guarded by the enclosing limiter lock, unlike the atomic delivery guard.
        boolean terminal;

        // Multiple threads may publish state transitions, but observers see them in order.
        void deliver() {
            do {
                if (!delivering.compareAndSet(false, true))
                    return;
                try {
                    Runnable next;
                    while ((next = deliveries.poll()) != null)
                        next.run();
                } finally {
                    delivering.set(false);
                }
            } while (!deliveries.isEmpty());
        }

        Entry(long enqueued, Consumer<RateLimitDecisionEvent> observer) {
            this.enqueued = enqueued;
            this.observer = observer;
        }

        /** Exposes observation/composition without exposing the internally writable future. */
        public CompletionStage<RateLimitDecision> decision() {
            LOG.info("event=method_invocation method=BoundedQueueRateLimiter.Entry.decision");
            return future.minimalCompletionStage();
        }

        /** Cancels this attempt if it is still pending; already granted admission is unchanged. */
        public void cancel() {
            LOG.info("event=method_invocation method=BoundedQueueRateLimiter.Entry.cancel");
            terminatePending(CANCELED, "CLIENT_CANCELLED");
        }

        /** Settles this attempt as timed out if it is still pending. */
        public void timeout() {
            LOG.info("event=method_invocation method=BoundedQueueRateLimiter.Entry.timeout");
            terminatePending(TIMED_OUT, "QUEUE_TIMEOUT");
        }

        /** Cancel/timeout competes with release under the same lock; only the first wins. */
        private void terminatePending(RateLimitDecision.Outcome outcome, String reason) {
            List<Runnable> notifications = new ArrayList<>();
            synchronized (BoundedQueueRateLimiter.this) {
                if (terminal)
                    return;
                int before = queue.size();
                queue.remove(this);
                long now = ticker.getAsLong();
                finish(this, outcome, reason, now, before, notifications);
                reschedule(now, notifications);
            }
            notifications.forEach(Runnable::run);
        }
    }
}
