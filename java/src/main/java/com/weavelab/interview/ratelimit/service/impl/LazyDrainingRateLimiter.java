package com.weavelab.interview.ratelimit.service.impl;

import com.weavelab.interview.config.RateLimitProperties;
import com.weavelab.interview.ratelimit.service.IRateLimiter;
import com.weavelab.interview.ratelimit.service.model.RateLimitDecision;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.function.LongSupplier;

/**
 * One process-local, globally shared lazy-draining bucket. Each admission adds one interval of
 * drain-time debt; elapsed monotonic time subtracts debt, down to zero. Capacity limits the initial
 * burst. No scheduler, queue or asynchronous result is needed. At capacity ten and six seconds per
 * request, ten requests fit immediately and one admission recovers every six seconds. This permits
 * bursts, not a strict rolling-minute quota.
 */
@Component
public final class LazyDrainingRateLimiter implements IRateLimiter {
    private static final Logger LOG = LoggerFactory.getLogger(LazyDrainingRateLimiter.class);
    private final long interval, maximumDebt;
    private final LongSupplier ticker;
    // Guarded by this; all arithmetic uses monotonic nanoseconds.
    private long debt, lastTick;

    /**
     * @param properties validated configuration shared by all requests
     */
    @Autowired
    public LazyDrainingRateLimiter(RateLimitProperties properties) {
        this(properties.getCapacity(), properties.interval(), System::nanoTime);
    }

    /**
     * Creates an empty bucket with injectable monotonic time for deterministic tests.
     *
     * @param capacity positive maximum burst size
     * @param interval positive drainage time per request
     * @param ticker monotonic nanosecond source
     * @throws IllegalArgumentException if settings are invalid or overflow nanosecond arithmetic
     */
    public LazyDrainingRateLimiter(int capacity, Duration interval, LongSupplier ticker) {
        LOG.info("event=method_invocation method=LazyDrainingRateLimiter.<init>");
        if (capacity <= 0
                || interval == null
                || interval.isZero()
                || interval.isNegative()
                || ticker == null)
            throw new IllegalArgumentException(
                    "Positive capacity and interval, and a monotonic ticker, are required");
        try {
            this.interval = interval.toNanos();
            this.maximumDebt = Math.multiplyExact(this.interval, capacity);
        } catch (ArithmeticException failure) {
            LOG.error("Lazy-draining capacity or interval exceeds supported bounds", failure);
            throw new IllegalArgumentException(
                    "Rate limit exceeds supported nanosecond bounds", failure);
        }
        this.ticker = ticker;
        this.lastTick = ticker.getAsLong();
    }

    /**
     * Atomically drains elapsed debt and admits when one more interval fits. Rejections do not add
     * debt; the retry delay is advisory under contention.
     *
     * @return immediate decision and immutable state snapshot for request logging
     */
    @Override
    public RateLimitDecision tryAcquire() {
        LOG.info("event=method_invocation method=LazyDrainingRateLimiter.tryAcquire");
        synchronized (this) {
            long now = ticker.getAsLong();
            long elapsed = Math.max(0, now - lastTick);
            long before = debt;
            debt = Math.max(0, debt - elapsed);
            lastTick = now;
            long drained = debt;
            boolean allowed = debt <= maximumDebt - interval;
            if (allowed) debt += interval;
            long retry = allowed ? 0 : debt - maximumDebt + interval;
            return new RateLimitDecision(
                    allowed,
                    Duration.ofNanos(retry),
                    before,
                    drained,
                    debt,
                    maximumDebt,
                    elapsed,
                    Instant.now());
        }
    }
}
