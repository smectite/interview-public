package com.weavelab.interview.ratelimit.service.impl;

import static com.weavelab.interview.ratelimit.service.model.RateLimitDecision.Outcome.*;

import com.weavelab.interview.ratelimit.service.IImmediateRateLimiter;
import com.weavelab.interview.ratelimit.service.model.RateLimitDecision;
import com.weavelab.interview.ratelimit.service.model.RateLimitDecisionEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * Process-local, thread-safe leaky bucket that computes drainage when a request arrives. No
 * scheduler, queue, background thread or asynchronous result is required.
 *
 * <p>Occupancy is represented as drain-time debt in nanoseconds rather than a request count. Each
 * admitted request adds one {@code interval} of debt. Elapsed monotonic time removes the same
 * amount of debt, down to zero. Capacity determines the maximum debt: {@code maximum = capacity *
 * interval}. Admission is possible when adding one interval would not exceed that maximum.
 *
 * <p>For capacity ten and a six-second interval, an empty bucket accepts ten requests immediately.
 * Its sixty seconds of accumulated debt then recovers one admission every six seconds. This allows
 * bursts and does not guarantee at most ten requests in every rolling minute; use the
 * sliding-window strategy when that guarantee is required.
 *
 * <p>State changes and diagnostic snapshots are protected by the same lock. Observer callbacks run
 * after releasing it. Rejections add no debt; idle time never creates credit beyond an empty
 * bucket. Admission limits starts, not concurrent execution.
 */
public final class LazyDrainingRateLimiter implements IImmediateRateLimiter {
    private static final Logger LOG = LoggerFactory.getLogger(LazyDrainingRateLimiter.class);

    // Both values are nanoseconds: cost per admission and maximum stored drain-time debt.
    private final long interval, maximum;
    private final LongSupplier ticker;
    // Guarded by this: outstanding drain-time debt and the ticker value at the last attempt.
    private long debt, last;
    private boolean closed;

    /**
     * Creates an initially empty bucket.
     *
     * @param capacity positive number of requests that fit in an empty bucket
     * @param interval positive drainage time per admitted request, e.g. six seconds
     * @param ticker monotonic nanosecond source, normally {@code System::nanoTime}; tests may
     *     inject a controllable ticker
     * @throws IllegalArgumentException if capacity or interval is not positive
     * @throws ArithmeticException if the duration or maximum debt cannot fit in a long
     */
    public LazyDrainingRateLimiter(int capacity, Duration interval, LongSupplier ticker) {
        LOG.info("event=method_invocation method=LazyDrainingRateLimiter.<init>");
        this.interval = LimiterSupport.interval(capacity, interval);
        this.maximum = Math.multiplyExact(this.interval, capacity);
        this.ticker = Objects.requireNonNull(ticker);
        last = ticker.getAsLong();
    }

    /**
     * Drains elapsed debt and returns ALLOWED, RATE_LIMITED or STOPPED immediately.
     *
     * <p>For a rejected request, retry-after is the additional drainage needed to fit one request:
     * {@code debt - maximum + interval}. It is advisory because another caller may acquire the
     * recovered space first. The immutable event describes this exact transition; observer runtime
     * exceptions do not change the admission decision.
     *
     * @param observer callback invoked outside the state lock, before returning
     * @return decision and suggested retry duration; no future or waiting is involved
     */
    public RateLimitDecision tryAcquire(Consumer<RateLimitDecisionEvent> observer) {
        LOG.info("event=method_invocation method=LazyDrainingRateLimiter.tryAcquire");
        RateLimitDecision.Outcome outcome;
        String reason;
        long retry;
        Map<String, Long> state;
        RateLimitDecisionEvent event;
        synchronized (this) {
            long now = ticker.getAsLong(), elapsed = Math.max(0, now - last), before = debt;
            // Catch up all drainage since the previous attempt; no timer updates are needed.
            debt = Math.max(0, debt - elapsed);
            last = now;
            long drained = debt;
            // Subtract first to express debt + interval <= maximum without addition overflow.
            outcome = closed ? STOPPED : debt <= maximum - interval ? ALLOWED : RATE_LIMITED;
            if (outcome == ALLOWED) debt += interval;
            retry = outcome == RATE_LIMITED ? debt - maximum + interval : 0;
            reason =
                    closed
                            ? "LIMITER_CLOSED"
                            : outcome == ALLOWED ? "AVAILABLE_CAPACITY" : "BUCKET_FULL";
            state =
                    Map.of(
                            "debtBeforeDrainNs",
                            before,
                            "debtAfterDrainNs",
                            drained,
                            "debtAfterDecisionNs",
                            debt,
                            "maxDebtNs",
                            maximum,
                            "elapsedNs",
                            elapsed);
            event = LimiterSupport.event(outcome, reason, retry, state);
        }

        // User callbacks and logging must not hold up other callers inside the state lock.
        return LimiterSupport.decide(observer, event);
    }

    /** Permanently stops admission. Repeated calls are harmless; existing work is unaffected. */
    public void close() {
        LOG.info("event=method_invocation method=LazyDrainingRateLimiter.close");
        synchronized (this) {
            closed = true;
        }
    }
}
