package com.weavelab.interview.ratelimit;

import static org.junit.jupiter.api.Assertions.*;

import com.weavelab.interview.ratelimit.strategy.impl.SlidingWindowRateLimiter;
import com.weavelab.interview.ratelimit.strategy.model.RateLimitDecision.Outcome;
import com.weavelab.interview.ratelimit.strategy.model.RateLimitDecisionEvent;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/** Verifies exact rolling expiration, concurrent limits and immutable snapshots. */
class SlidingWindowRateLimiterTest {
    @Test
    void expiresAdmissionsIndividuallyAtExactRollingBoundary() {
        var time = new AtomicLong();
        var limiter = new SlidingWindowRateLimiter(10, Duration.ofMinutes(1), time::get);
        for (int i = 0; i < 5; i++) assertEquals(Outcome.ALLOWED, outcome(limiter));
        time.set(Duration.ofSeconds(30).toNanos());
        for (int i = 0; i < 5; i++) assertEquals(Outcome.ALLOWED, outcome(limiter));
        time.set(Duration.ofSeconds(60).toNanos() - 1);
        var rejected = limiter.tryAcquire();
        assertEquals(Outcome.RATE_LIMITED, rejected.outcome());
        assertEquals(Duration.ofNanos(1), rejected.retryAfter());
        time.incrementAndGet();
        for (int i = 0; i < 5; i++) assertEquals(Outcome.ALLOWED, outcome(limiter));
        assertEquals(Outcome.RATE_LIMITED, outcome(limiter));
        time.set(Duration.ofSeconds(90).toNanos());
        for (int i = 0; i < 5; i++) assertEquals(Outcome.ALLOWED, outcome(limiter));
        assertEquals(Outcome.RATE_LIMITED, outcome(limiter));
    }

    @Test
    void concurrentCallersShareTenAdmissions() throws Exception {
        var limiter = new SlidingWindowRateLimiter(10, Duration.ofMinutes(1), () -> 0L);
        var pool = Executors.newFixedThreadPool(16);
        try {
            var start = new CountDownLatch(1);
            var attempts = new java.util.ArrayList<Future<Outcome>>();
            for (int i = 0; i < 100; i++)
                attempts.add(
                        pool.submit(
                                () -> {
                                    start.await();
                                    return outcome(limiter);
                                }));
            start.countDown();
            int allowed = 0;
            for (var attempt : attempts)
                if (attempt.get(5, TimeUnit.SECONDS) == Outcome.ALLOWED) allowed++;
            assertEquals(10, allowed);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void snapshotsRemainImmutableObserverFailureDoesNotChangeAdmissionAndCloseStops() {
        var time = new AtomicLong();
        var limiter = new SlidingWindowRateLimiter(10, Duration.ofMinutes(1), time::get);
        var events = new java.util.ArrayList<RateLimitDecisionEvent>();
        limiter.tryAcquire(events::add);
        assertEquals(1L, events.get(0).state().get("windowCountAfter"));
        assertThrows(UnsupportedOperationException.class, () -> events.get(0).state().put("x", 1L));
        assertEquals(
                Outcome.ALLOWED,
                limiter.tryAcquire(
                                e -> {
                                    throw new IllegalStateException();
                                })
                        .outcome());
        time.set(Duration.ofHours(2).toNanos());
        assertEquals(Outcome.ALLOWED, outcome(limiter));
        assertEquals(1L, events.get(0).state().get("windowCountAfter"));
        limiter.close();
        assertEquals(Outcome.STOPPED, outcome(limiter));
    }

    private static Outcome outcome(SlidingWindowRateLimiter limiter) {
        return limiter.tryAcquire().outcome();
    }
}
