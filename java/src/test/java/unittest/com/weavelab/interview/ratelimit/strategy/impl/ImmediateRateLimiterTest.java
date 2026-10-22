package unittest.com.weavelab.interview.ratelimit.strategy.impl;

import static org.junit.jupiter.api.Assertions.*;

import com.weavelab.interview.ratelimit.service.IImmediateRateLimiter;
import com.weavelab.interview.ratelimit.service.impl.LazyDrainingRateLimiter;
import com.weavelab.interview.ratelimit.service.impl.SlidingWindowRateLimiter;
import com.weavelab.interview.ratelimit.service.model.RateLimitDecision;
import com.weavelab.interview.ratelimit.service.model.RateLimitDecisionEvent;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;

/** Verifies the direct-decision contract shared by immediate admission algorithms. */
class ImmediateRateLimiterTest {
    /**
     * Both immediate implementations return direct decisions, notify once per admission, reject
     * exhaustion and return STOPPED after close.
     */
    @Test
    void immediateAlgorithmsReturnDecisionsDirectlyAndStopAfterClose() {
        for (IImmediateRateLimiter limiter :
                new IImmediateRateLimiter[] {
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

    /**
     * Both immediate algorithms notify outside the state lock and permit safe reentrant admission.
     */
    @Test
    void observersRunOutsideMonitorAndCanReenter() {
        for (IImmediateRateLimiter limiter :
                new IImmediateRateLimiter[] {
                    new LazyDrainingRateLimiter(10, Duration.ofSeconds(6), () -> 0L),
                    new SlidingWindowRateLimiter(10, Duration.ofMinutes(1), () -> 0L)
                }) {
            var events = new ArrayList<RateLimitDecisionEvent>();
            var first =
                    limiter.tryAcquire(
                            event -> {
                                assertFalse(Thread.holdsLock(limiter));
                                events.add(event);
                                assertEquals(
                                        RateLimitDecision.Outcome.ALLOWED,
                                        limiter.tryAcquire().outcome());
                            });
            assertEquals(RateLimitDecision.Outcome.ALLOWED, first.outcome());
            assertEquals(1, events.size());
            var snapshot = events.get(0).state();
            if (limiter instanceof LazyDrainingRateLimiter)
                assertEquals(6_000_000_000L, snapshot.get("debtAfterDecisionNs"));
            else assertEquals(1L, snapshot.get("windowCountAfter"));
            for (int i = 0; i < 8; i++)
                assertEquals(RateLimitDecision.Outcome.ALLOWED, limiter.tryAcquire().outcome());
            assertEquals(RateLimitDecision.Outcome.RATE_LIMITED, limiter.tryAcquire().outcome());
            limiter.close();
        }
    }
}
