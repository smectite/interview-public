package com.weavelab.interview.ratelimit.service.model;

import java.time.Duration;

/**
 * Immutable terminal admission result.
 *
 * @param outcome admission or terminal lifecycle outcome
 * @param retryAfter advisory delay before retrying a rejection; another caller may consume
 *     availability first
 */
public record RateLimitDecision(Outcome outcome, Duration retryAfter) {
    /** Terminal states of an admission attempt. */
    public enum Outcome {
        /** Admission granted; the caller may start downstream work. */
        ALLOWED,
        /** No immediate allowance or waiting capacity is available. */
        RATE_LIMITED,
        /** The pending admission reached its waiting deadline. */
        TIMED_OUT,
        /** The limiter is closed or cannot safely progress pending work. */
        STOPPED,
        /** The caller canceled the pending admission. */
        CANCELED
    }
}
