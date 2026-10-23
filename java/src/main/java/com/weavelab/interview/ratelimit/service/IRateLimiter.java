package com.weavelab.interview.ratelimit.service;

import com.weavelab.interview.ratelimit.service.model.RateLimitDecision;

/**
 * Synchronous, thread-safe admission contract injected into HTTP adapters. Implementations own
 * their rate state and return a decision immediately, without queueing.
 */
@FunctionalInterface
public interface IRateLimiter {
    /**
     * Atomically attempts to acquire one admission.
     *
     * @return immediate decision, advisory retry delay and immutable diagnostic snapshot
     */
    RateLimitDecision tryAcquire();
}
