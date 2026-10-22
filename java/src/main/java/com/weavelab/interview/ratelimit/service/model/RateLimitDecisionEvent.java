package com.weavelab.interview.ratelimit.strategy.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable diagnostic snapshot of one admission or queue transition. The state map is defensively
 * copied; timestamps are diagnostic, never used for rate arithmetic.
 *
 * @param decision transition name, including ENQUEUED or a terminal outcome
 * @param reason stable explanation of the transition
 * @param retryAfter suggested delay for retrying a rejected request
 * @param waited elapsed monotonic waiting duration
 * @param state immutable strategy-specific state with units in field names
 * @param decisionTime wall-clock instant captured when the transition is recorded
 */
public record RateLimitDecisionEvent(
        String decision,
        String reason,
        Duration retryAfter,
        Duration waited,
        Map<String, Long> state,
        Instant decisionTime) {
    private static final Logger LOG = LoggerFactory.getLogger(RateLimitDecisionEvent.class);

    /**
     * Constructs an immutable snapshot; the convenience overload captures the current wall-clock
     * time.
     */
    public RateLimitDecisionEvent {
        LOG.info("event=method_invocation method=RateLimitDecisionEvent.<init>");
        Objects.requireNonNull(decision);
        Objects.requireNonNull(reason);
        Objects.requireNonNull(retryAfter);
        Objects.requireNonNull(waited);
        Objects.requireNonNull(decisionTime);
        state = Map.copyOf(state);
    }

    /**
     * Constructs an immutable snapshot; the convenience overload captures the current wall-clock
     * time.
     */
    public RateLimitDecisionEvent(
            String decision,
            String reason,
            Duration retryAfter,
            Duration waited,
            Map<String, Long> state) {
        this(decision, reason, retryAfter, waited, state, Instant.now());
        LOG.info("event=method_invocation method=RateLimitDecisionEvent.<init>");
    }
}
