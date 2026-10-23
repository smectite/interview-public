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
 *
 * <p>For example, capacity 2 permits two immediate admissions and two additional waiting
 * requests. With a one-second interval, one waiting request is released per second after the
 * initial burst, provided its timeout has not elapsed. This controls admission rate, not the
 * number of admitted requests still executing. Limits apply only to this instance; separate
 * instances or application processes do not share tokens.
 *
 * <p>Each attempt has exactly one terminal outcome: ALLOWED, RATE_LIMITED (queue full),
 * TIMED_OUT, CANCELED or STOPPED. Closing the limiter or a scheduler rejection permanently stops
 * admission. No worker thread sleeps while waiting for a token, although observer callbacks and
 * synchronous completion-stage continuations may run on the submitting, scheduler or closing
 * thread and should return promptly.
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
     * <p>Durations are converted to nanoseconds. Initialization samples the ticker once to
     * establish the refill baseline; subsequent elapsed-time calculations use that same clock.
     *
     * @throws IllegalArgumentException if capacity, interval or timeout is not positive
     * @throws ArithmeticException if a duration or capacity times interval overflows nanoseconds
     * @throws NullPointerException if a duration, ticker or scheduler is null
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
     *
     * <p>Under the limiter lock, first refill tokens, expire overdue entries and release older
     * waiting requests. Only then consider this attempt: take a token if the queue is empty,
     * reject if the waiting queue is full, or append to its tail. Refresh the scheduled wake-up
     * after this transition. A closed limiter immediately returns STOPPED. Notifications collected
     * during the transition are delivered after releasing the lock; concurrent delivery can mean
     * the returned handle is still awaiting notification even after its outcome has been chosen.
     *
     * @param observer receives immutable diagnostic events; runtime exceptions are logged and
     *     isolated from the admission decision
     * @return handle for observing the terminal decision or terminating pending admission
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
     *
     * <p>Cancel the previous task without interrupting a callback already running. Schedule no
     * replacement when the queue is empty or the limiter is closed. Otherwise choose the minimum
     * of the next-token delay and every waiting entry's remaining timeout, clamped to zero.
     * An early or late callback rechecks actual clock time in {@link #tick()}.
     *
     * <p>If the executor rejects the task, permanently stop the limiter and collect STOPPED
     * completions for all pending entries; the caller delivers those completions outside the lock.
     *
     * @param now current monotonic time in nanoseconds
     * @param notifications destination for deferred logging and terminal delivery
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

    /**
     * Refills the bucket, removes every expired entry, then admits surviving entries in FIFO order.
     * The caller must hold the limiter lock and deliver collected notifications after unlocking.
     *
     * <p>An entry expires when its age is greater than or equal to the timeout. Expiration is
     * checked before any token is spent, so timeout wins at an exact deadline. Expired entries
     * consume no token. The release loop can spend several accumulated tokens after a delayed
     * callback, taking one token per admitted entry.
     *
     * @param now current monotonic time in nanoseconds
     * @param notifications destination for deferred observer calls and future completions
     */
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

    /**
     * Credits whole elapsed intervals to the bucket without exceeding capacity; caller holds the lock.
     *
     * <p>If already full, move the baseline to now so idle time cannot fund an extra burst. Otherwise
     * divide nonnegative elapsed time by the interval. With no whole interval, leave state unchanged.
     * If the earned tokens fill the bucket, reset the baseline to now and discard excess time.
     * For a partial refill, advance the baseline by only the credited whole intervals, preserving
     * the fractional remainder for the next call. Backward clock movement earns no tokens.
     *
     * @param now current monotonic time in nanoseconds
     */
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

    /**
     * Calculates the delay until a token can be used, without modifying refill state.
     * The caller must hold the limiter lock and normally refill before making scheduling decisions.
     *
     * @param now current monotonic time in nanoseconds
     * @return zero if tokens exist or an interval has elapsed; otherwise the remaining nanoseconds
     *     in the interval, treating backward clock movement as zero elapsed time
     */
    private long nextTokenDelay(long now) {
        return tokens > 0 ? 0 : Math.max(0, interval - Math.max(0, now - lastRefill));
    }

    /**
     * Handles one scheduler wake-up using actual elapsed time rather than assuming it ran on time.
     *
     * <p>Under the limiter lock, return if closed; otherwise process expiration and admission,
     * then schedule the next useful wake-up. Deliver collected events and completions outside the
     * lock. This makes early callbacks harmless and lets late callbacks use accumulated tokens.
     */
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

    /**
     * Builds an immutable diagnostic snapshot while the caller holds the limiter lock.
     *
     * <p>The supplied queue size records the state before the transition; the current size and
     * token count record the state after it. Request age and deadline remainder are clamped to
     * zero. Taking this snapshot now lets later observer delivery avoid reading mutable state.
     *
     * @param e attempt whose elapsed wait and deadline are reported
     * @param decision event name, including ENQUEUED or a terminal outcome
     * @param reason machine-readable reason for the transition
     * @param now current monotonic time in nanoseconds
     * @param before queue size immediately before the transition
     * @param retry advisory retry delay in nanoseconds, normally zero except for queue rejection
     * @return event with timing, queue bounds, token bounds and next-token delay
     */
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
     *
     * <p>Set the terminal flag immediately so release, cancellation and timeout cannot choose
     * another outcome. Capture diagnostics and the result, then append one delivery action to the
     * entry mailbox. That action emits the terminal event before completing the future. Earlier
     * ENQUEUED delivery stays ahead of it even if another thread drains the mailbox first.
     * This method neither removes the entry from the waiting queue nor spends a token; callers
     * perform those state changes first. Only RATE_LIMITED receives an advisory retry delay.
     *
     * @param e nonterminal attempt to settle
     * @param outcome final admission outcome
     * @param reason machine-readable reason for the outcome
     * @param now current monotonic time in nanoseconds
     * @param before queue size before the caller's transition
     * @param notifications destination for the deferred mailbox drain
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

    /**
     * Permanently closes admission and drains pending entries as STOPPED under the limiter lock.
     *
     * <p>Cancel the scheduled wake-up without interrupting a running callback, clear its handle,
     * and remove waiting entries in FIFO order. Each removal records its own queue-size transition.
     * Repeated calls are harmless because the queue is already empty. The externally owned executor
     * and previously admitted work remain active.
     *
     * @param now current monotonic time used for all shutdown snapshots
     * @param notifications destination for terminal deliveries to run after unlocking
     */
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
     *
     * <p>Shutdown competes with admission and cancellation under the same lock. Once stopped,
     * future submissions receive STOPPED. Repeated calls are harmless. Deliver pending terminal
     * events and complete their futures after releasing the lock.
     */
    public void close() {
        LOG.info("event=method_invocation method=BoundedQueueRateLimiter.close");
        List<Runnable> notifications = new ArrayList<>();
        synchronized (this) {
            stop(ticker.getAsLong(), notifications);
        }
        notifications.forEach(Runnable::run);
    }

    /**
     * State and completion handle for one admission attempt, including immediate decisions.
     *
     * <p>The timestamp is captured at submission and becomes the deadline baseline if queued.
     * Queue membership and terminal state use the enclosing limiter lock. The concurrent mailbox
     * and atomic drain guard serialize this entry's notifications outside that lock; different
     * entries may deliver concurrently.
     */
    private final class Entry implements ISubmission {
        final long enqueued;
        final Consumer<RateLimitDecisionEvent> observer;
        final CompletableFuture<RateLimitDecision> future = new CompletableFuture<>();
        // Publication occurs under the limiter lock; delivery can run concurrently outside it.
        final Queue<Runnable> deliveries = new ConcurrentLinkedQueue<>();
        final AtomicBoolean delivering = new AtomicBoolean();
        // Guarded by the enclosing limiter lock, unlike the atomic delivery guard.
        boolean terminal;

        /**
         * Drains this entry's notification mailbox in publication order outside the limiter lock.
         *
         * <p>An atomic compare-and-set gives one thread ownership; competing drainers return.
         * Always release ownership in a finally block. Recheck after releasing it to cover a
         * publisher that appended work while a competing drainer still saw ownership held.
         * Another thread may acquire ownership first, in which case this drainer exits safely.
         * This preserves ENQUEUED before the terminal event and terminal observer delivery before
         * future completion, including when callbacks reenter the limiter.
         */
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

        /**
         * Creates a pending attempt with an empty mailbox and incomplete decision future.
         *
         * @param enqueued submission time in monotonic nanoseconds, also the queue deadline baseline
         * @param observer diagnostic event consumer, invoked only through deferred delivery
         */
        Entry(long enqueued, Consumer<RateLimitDecisionEvent> observer) {
            this.enqueued = enqueued;
            this.observer = observer;
        }

        /**
         * Exposes the terminal result as a completion stage suitable for asynchronous composition.
         *
         * <p>The minimal stage does not expose the internally writable future. Completing or
         * canceling a future obtained from that stage does not change this attempt. Use
         * {@link #cancel()} or {@link #timeout()} to terminate pending admission. Terminal observer
         * delivery occurs before the internal future completes; callers should not block waiting
         * for this stage, especially from the observer itself.
         *
         * @return observation stage for the single final decision
         */
        public CompletionStage<RateLimitDecision> decision() {
            LOG.info("event=method_invocation method=BoundedQueueRateLimiter.Entry.decision");
            return future.minimalCompletionStage();
        }

        /**
         * Removes pending admission and reports CANCELED with reason CLIENT_CANCELLED.
         *
         * <p>This is a normal decision completion, not exceptional cancellation of the future.
         * Removal frees a waiting slot and refreshes scheduling without consuming or refunding a
         * token. If any terminal transition already won, including admission, this call does nothing.
         */
        public void cancel() {
            LOG.info("event=method_invocation method=BoundedQueueRateLimiter.Entry.cancel");
            terminatePending(CANCELED, "CLIENT_CANCELLED");
        }

        /**
         * Removes pending admission and reports TIMED_OUT with reason QUEUE_TIMEOUT.
         *
         * <p>This explicit signal may come from an adapter timeout; it does not check whether the
         * limiter's enqueue-relative deadline has elapsed. Removal frees a waiting slot without
         * spending a token. An already terminal attempt is unchanged.
         */
        public void timeout() {
            LOG.info("event=method_invocation method=BoundedQueueRateLimiter.Entry.timeout");
            terminatePending(TIMED_OUT, "QUEUE_TIMEOUT");
        }

        /**
         * Applies cancellation or an explicit timeout atomically against admission and shutdown.
         *
         * <p>Under the enclosing limiter lock, return if already terminal; otherwise remove this
         * entry, capture current time, record its terminal transition and refresh the wake-up.
         * This method does not refill or release other entries directly; a zero-delay wake-up can
         * process available tokens. Drain notifications after unlocking so observers can reenter.
         *
         * @param outcome CANCELED or TIMED_OUT, completed as a normal decision
         * @param reason machine-readable cause of the explicit termination
         */
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
