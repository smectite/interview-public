package com.weavelab.interview.ratelimit.service.model;

import java.time.Duration;
import java.time.Instant;

/**
 * Immutable snapshot captured with the admission transition; logging occurs outside the lock.
 *
 * @param allowed whether downstream processing may begin
 * @param retryAfter drainage required before a rejected request could fit
 * @param debtBeforeDrainNs debt before accounting for elapsed time
 * @param debtAfterDrainNs remaining debt before this admission
 * @param debtAfterDecisionNs debt after the decision
 * @param maxDebtNs maximum stored drain-time debt
 * @param elapsedNs monotonic elapsed time used for drainage
 * @param decisionTime diagnostic wall-clock time, never used for rate arithmetic
 */
public record RateLimitDecision(
        boolean allowed,
        Duration retryAfter,
        long debtBeforeDrainNs,
        long debtAfterDrainNs,
        long debtAfterDecisionNs,
        long maxDebtNs,
        long elapsedNs,
        Instant decisionTime) {}
