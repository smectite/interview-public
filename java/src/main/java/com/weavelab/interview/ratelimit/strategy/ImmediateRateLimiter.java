package com.weavelab.interview.ratelimit.strategy;

import com.weavelab.interview.ratelimit.strategy.model.RateLimitDecision;
import com.weavelab.interview.ratelimit.strategy.model.RateLimitDecisionEvent;

import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

/**
 * Thread-safe immediate admission without queueing or asynchronous completion. Observers receive
 * immutable snapshots outside the state lock before the call returns; observer runtime exceptions
 * do not change admission. Attempts after close return STOPPED.
 */
public interface ImmediateRateLimiter extends RateLimiter {
    /**
     * Attempts immediate admission.
     *
     * @param observer diagnostic callback for this attempt
     * @return terminal admission decision with an advisory retry duration
     */
    RateLimitDecision tryAcquire(Consumer<RateLimitDecisionEvent> observer);

    /**
     * Attempts immediate admission without observing the diagnostic snapshot.
     *
     * @return terminal admission decision
     */
    default RateLimitDecision tryAcquire() {
        LoggerFactory.getLogger(ImmediateRateLimiter.class)
                .info("event=method_invocation method=ImmediateRateLimiter.tryAcquire()");
        return tryAcquire(event -> {});
    }
}
