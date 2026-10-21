package com.weavelab.interview.ratelimit.strategy;

import com.weavelab.interview.ratelimit.strategy.model.RateLimitDecision;

import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletionStage;

/**
 * Handle for one admission attempt, which may already have completed. Cancellation and timeout
 * affect pending work only, never undoing a granted admission.
 */
public interface Submission {
    /**
     * Observes the one terminal decision without exposing its internally writable future.
     *
     * @return observable completion; servlet request threads must not block waiting for it
     */
    CompletionStage<RateLimitDecision> decision();

    /** Cancels pending admission; repeated calls do not change a terminal decision. */
    void cancel();

    /**
     * Expires pending admission. This default falls back to cancellation; implementations that
     * distinguish deadlines override it to report TIMED_OUT.
     */
    default void timeout() {
        LoggerFactory.getLogger(Submission.class)
                .info("event=method_invocation method=Submission.timeout()");
        cancel();
    }
}
