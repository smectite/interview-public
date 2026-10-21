package com.weavelab.interview.ratelimit;

import static org.junit.jupiter.api.Assertions.*;

import com.weavelab.interview.ratelimit.strategy.ImmediateRateLimiter;
import com.weavelab.interview.ratelimit.strategy.impl.LazyDrainingRateLimiter;
import com.weavelab.interview.ratelimit.strategy.impl.SlidingWindowRateLimiter;
import com.weavelab.interview.ratelimit.strategy.model.RateLimitDecision;
import com.weavelab.interview.ratelimit.strategy.model.RateLimitDecisionEvent;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;

/** Verifies the direct-decision contract shared by immediate admission algorithms. */
class ImmediateRateLimiterTest {
    @Test
    void immediateAlgorithmsReturnDecisionsDirectlyAndStopAfterClose() {
        for (ImmediateRateLimiter limiter :
                new ImmediateRateLimiter[] {
                    new LazyDrainingRateLimiter(10, Duration.ofSeconds(6), () -> 0L),
                    new SlidingWindowRateLimiter(10, Duration.ofMinutes(1), () -> 0L)
                }) {
            var events = new ArrayList<RateLimitDecisionEvent>();
            for (int i = 0; i < 10; i++) {
                RateLimitDecision decision = limiter.tryAcquire(events::add);
                assertEquals(RateLimitDecision.Outcome.ALLOWED, decision.outcome());
            }
            assertEquals(10, events.size());
            assertEquals(RateLimitDecision.Outcome.RATE_LIMITED, limiter.tryAcquire().outcome());
            limiter.close();
            assertEquals(RateLimitDecision.Outcome.STOPPED, limiter.tryAcquire().outcome());
        }
    }
}
