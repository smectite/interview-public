package unittest.com.weavelab.interview.ratelimit.strategy.impl;

import static org.junit.jupiter.api.Assertions.*;

import com.weavelab.interview.ratelimit.service.impl.SlidingWindowRateLimiter;
import com.weavelab.interview.ratelimit.service.model.RateLimitDecision.Outcome;
import com.weavelab.interview.ratelimit.service.model.RateLimitDecisionEvent;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** Verifies exact rolling expiration, concurrent limits and immutable snapshots. */
class SlidingWindowRateLimiterTest {
    /**
     * Two staggered batches expire independently; just-before, exact sixty-second and later ninety-
     * second boundaries preserve the quota and exact retry.
     */
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

    /** At a fixed time, exactly ten of 100 concurrent sliding-window callers succeed. */
    @Test
    void concurrentCallersShareTenAdmissions() throws Exception {
        var limiter = new SlidingWindowRateLimiter(10, Duration.ofMinutes(1), () -> 0L);
        var pool = Executors.newFixedThreadPool(16);
        try {
            var start = new CountDownLatch(1);
            var attempts = new ArrayList<Future<Outcome>>();
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

    /**
     * Sliding snapshots remain immutable/stable; observer failure preserves admission; long idle
     * permits reuse; close stops new admission.
     */
    @Test
    void snapshotsRemainImmutableObserverFailureDoesNotChangeAdmissionAndCloseStops() {
        var time = new AtomicLong();
        var limiter = new SlidingWindowRateLimiter(10, Duration.ofMinutes(1), time::get);
        var events = new ArrayList<RateLimitDecisionEvent>();
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

    /**
     * Concurrent callers cannot reuse the single slot freed by expiration of a staggered request.
     */
    @Test
    void concurrentCallersShareOnlyNewlyExpiredSlot() throws Exception {
        var clock = new ManualScheduler();
        var limiter = new SlidingWindowRateLimiter(10, Duration.ofMinutes(1), clock.time::get);
        limiter.tryAcquire();
        clock.advance(1_000_000_000L);
        for (int i = 0; i < 9; i++) limiter.tryAcquire();
        clock.time.set(60_000_000_000L);
        var pool = Executors.newFixedThreadPool(16);
        try {
            var start = new CountDownLatch(1);
            var results = new ArrayList<Future<Boolean>>();
            for (int i = 0; i < 100; i++)
                results.add(
                        pool.submit(
                                () -> {
                                    start.await();
                                    return limiter.tryAcquire().outcome()
                                            == com.weavelab.interview.ratelimit.service.model
                                                    .RateLimitDecision.Outcome.ALLOWED;
                                }));
            start.countDown();
            int allowed = 0;
            for (var result : results) if (result.get(5, TimeUnit.SECONDS)) allowed++;
            assertEquals(1, allowed);
        } finally {
            pool.shutdownNow();
            limiter.close();
        }
    }
}
