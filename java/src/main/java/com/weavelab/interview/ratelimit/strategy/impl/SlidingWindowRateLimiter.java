package com.weavelab.interview.ratelimit.strategy.impl;

import com.weavelab.interview.ratelimit.strategy.ImmediateRateLimiter;
import com.weavelab.interview.ratelimit.strategy.model.RateLimitDecision;
import com.weavelab.interview.ratelimit.strategy.model.RateLimitDecision.Outcome;
import com.weavelab.interview.ratelimit.strategy.model.RateLimitDecisionEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/** Exact accepted-request log for the rolling interval (now - period, now]. */
public final class SlidingWindowRateLimiter implements ImmediateRateLimiter {
    private static final Logger LOG = LoggerFactory.getLogger(SlidingWindowRateLimiter.class);

    private final int requests;
    private final long period;
    private final LongSupplier ticker;
    private final Deque<Long> admissions = new ArrayDeque<>();
    private boolean closed;

    /** Creates the configured adapter or value using the supplied dependencies. */
    public SlidingWindowRateLimiter(int requests, Duration period, LongSupplier ticker) {
        LOG.info("event=method_invocation method=SlidingWindowRateLimiter.<init>");
        if (requests <= 0 || period.toNanos() <= 0)
            throw new IllegalArgumentException("Requests and period must be positive");
        this.requests = requests;
        this.period = period.toNanos();
        this.ticker = Objects.requireNonNull(ticker);
    }

    /**
     * Expires old admissions and returns one immediate decision, notifying the observer outside the
     * lock.
     */
    @Override
    public RateLimitDecision tryAcquire(Consumer<RateLimitDecisionEvent> observer) {
        LOG.info("event=method_invocation method=SlidingWindowRateLimiter.tryAcquire");
        RateLimitDecisionEvent event;
        synchronized (this) {
            long now = ticker.getAsLong();
            int before = admissions.size();
            while (!admissions.isEmpty() && now - admissions.peekFirst() >= period)
                admissions.removeFirst();
            Outcome outcome;
            long retry = 0;
            String reason;
            if (closed) {
                outcome = Outcome.STOPPED;
                reason = "LIMITER_CLOSED";
            } else if (admissions.size() < requests) {
                admissions.addLast(now);
                outcome = Outcome.ALLOWED;
                reason = "AVAILABLE_CAPACITY";
            } else {
                outcome = Outcome.RATE_LIMITED;
                reason = "WINDOW_FULL";
                retry = period - (now - admissions.peekFirst());
            }
            event =
                    LimiterSupport.event(
                            outcome,
                            reason,
                            retry,
                            Map.of(
                                    "windowCountBefore",
                                    (long) before,
                                    "windowCountAfter",
                                    (long) admissions.size(),
                                    "windowLimit",
                                    (long) requests,
                                    "windowPeriodNs",
                                    period));
        }
        return LimiterSupport.decide(observer, event);
    }

    /** Permanently stops admission and clears retained state. */
    @Override
    public void close() {
        LOG.info("event=method_invocation method=SlidingWindowRateLimiter.close");
        synchronized (this) {
            closed = true;
            admissions.clear();
        }
    }
}
