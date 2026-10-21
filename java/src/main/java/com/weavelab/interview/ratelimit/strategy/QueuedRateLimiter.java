package com.weavelab.interview.ratelimit.strategy;

import com.weavelab.interview.ratelimit.strategy.model.RateLimitDecisionEvent;

import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

/**
 * Thread-safe admission that may complete immediately or wait for an allowance. Cancel and timeout
 * settle pending work at most once; close settles it as STOPPED. Observers run outside the state
 * lock, with ENQUEUED delivered before its terminal event.
 */
public interface QueuedRateLimiter extends RateLimiter {
    /**
     * Attempts immediate admission or retains the request for later completion.
     *
     * @param observer ordered diagnostic callback for this request
     * @return completion and cancellation handle, possibly already settled
     */
    Submission submit(Consumer<RateLimitDecisionEvent> observer);

    /**
     * Submits an admission attempt without observing diagnostic transitions.
     *
     * @return completion and cancellation handle
     */
    default Submission submit() {
        LoggerFactory.getLogger(QueuedRateLimiter.class)
                .info("event=method_invocation method=QueuedRateLimiter.submit()");
        return submit(event -> {});
    }
}
