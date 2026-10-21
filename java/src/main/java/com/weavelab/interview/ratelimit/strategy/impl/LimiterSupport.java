package com.weavelab.interview.ratelimit.strategy.impl;

import com.weavelab.interview.ratelimit.strategy.model.RateLimitDecision;
import com.weavelab.interview.ratelimit.strategy.model.RateLimitDecisionEvent;

import java.time.Duration;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Internal validation, immutable event construction and failure-isolated observer delivery. Helpers
 * preserve admission outcomes when diagnostic observers fail.
 */
final class LimiterSupport {
    private static final org.slf4j.Logger LOG =
            org.slf4j.LoggerFactory.getLogger(LimiterSupport.class);

    private LimiterSupport() {}

    static long interval(int capacity, Duration duration) {
        if (capacity < 1) throw new IllegalArgumentException("Capacity must be bigger than 0");
        long nanos = duration.toNanos();
        if (nanos <= 0) throw new IllegalArgumentException("Interval must be positive");
        Math.multiplyExact(nanos, capacity);

        return nanos;
    }

    static void emit(Consumer<RateLimitDecisionEvent> observer, RateLimitDecisionEvent event) {
        try {
            observer.accept(event);
        } catch (RuntimeException failure) {
            LOG.warn("Rate limit observer failed; admission decision is preserved", failure);
        }
    }

    static RateLimitDecisionEvent event(
            RateLimitDecision.Outcome outcome, String reason, long retry, Map<String, Long> state) {
        return new RateLimitDecisionEvent(
                outcome.name(), reason, Duration.ofNanos(Math.max(0, retry)), Duration.ZERO, state);
    }

    static RateLimitDecision decide(
            Consumer<RateLimitDecisionEvent> observer, RateLimitDecisionEvent event) {
        var decision =
                new RateLimitDecision(
                        RateLimitDecision.Outcome.valueOf(event.decision()), event.retryAfter());
        emit(observer, event);

        return decision;
    }
}
