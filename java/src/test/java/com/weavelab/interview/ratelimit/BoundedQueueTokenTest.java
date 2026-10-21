package com.weavelab.interview.ratelimit;

import static org.junit.jupiter.api.Assertions.*;

import com.weavelab.interview.ratelimit.strategy.Submission;
import com.weavelab.interview.ratelimit.strategy.impl.BoundedQueueRateLimiter;
import com.weavelab.interview.ratelimit.strategy.model.RateLimitDecision.Outcome;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;

/** Exercises token bursts, refill accounting, bounded FIFO waiting and concurrency. */
class BoundedQueueTokenTest {
    private BoundedQueueRateLimiter limiter(ManualScheduler clock, int capacity, int timeout) {
        return new BoundedQueueRateLimiter(
                capacity,
                Duration.ofSeconds(6),
                Duration.ofSeconds(timeout),
                clock.time::get,
                clock);
    }

    @Test
    void initialTokensAllowTenThenQueueAndRejectOverflow() {
        var clock = new ManualScheduler();
        var limiter = limiter(clock, 10, 75);
        for (int i = 0; i < 10; i++) {
            var request = limiter.submit();
            assertTrue(request.decision().toCompletableFuture().isDone());
            assertEquals(Outcome.ALLOWED, outcome(request));
        }
        var waiting = new ArrayList<Submission>();
        for (int i = 0; i < 10; i++) {
            waiting.add(limiter.submit());
            assertFalse(waiting.get(i).decision().toCompletableFuture().isDone());
        }
        assertEquals(Outcome.RATE_LIMITED, outcome(limiter.submit()));
        clock.advance(6_000_000_000L);
        assertEquals(Outcome.ALLOWED, outcome(waiting.get(0)));
        assertFalse(waiting.get(1).decision().toCompletableFuture().isDone());
        limiter.close();
    }

    @Test
    void idleRefillCapsAtCapacityAndPreservesPartialRefillTime() {
        var clock = new ManualScheduler();
        var limiter = limiter(clock, 10, 75);
        for (int i = 0; i < 10; i++) limiter.submit();
        clock.advance(9_000_000_000L);
        assertEquals(Outcome.ALLOWED, outcome(limiter.submit()));
        var waiting = limiter.submit();
        clock.advance(2_999_999_999L);
        assertFalse(waiting.decision().toCompletableFuture().isDone());
        clock.advance(1);
        assertEquals(Outcome.ALLOWED, outcome(waiting));
        clock.advance(600_000_000_000L);
        for (int i = 0; i < 10; i++) assertEquals(Outcome.ALLOWED, outcome(limiter.submit()));
        assertFalse(limiter.submit().decision().toCompletableFuture().isDone());
        limiter.close();
    }

    @Test
    void newArrivalCannotBypassOlderQueueWhenSchedulerIsLate() {
        var clock = new ManualScheduler();
        var limiter = limiter(clock, 1, 75);
        limiter.submit();
        var first = limiter.submit();
        clock.time.set(6_000_000_000L);
        var newcomer = limiter.submit();
        assertEquals(Outcome.ALLOWED, outcome(first));
        assertFalse(newcomer.decision().toCompletableFuture().isDone());
        clock.advance(6_000_000_000L);
        assertEquals(Outcome.ALLOWED, outcome(newcomer));
    }

    @Test
    void expiredRequestDoesNotConsumeNewToken() {
        var clock = new ManualScheduler();
        var limiter = limiter(clock, 1, 6);
        limiter.submit();
        var waiting = limiter.submit();
        clock.advance(6_000_000_000L);
        assertEquals(Outcome.TIMED_OUT, outcome(waiting));
        assertEquals(Outcome.ALLOWED, outcome(limiter.submit()));
    }

    @Test
    void concurrentCallersCannotOverspendTokensOrOverflowQueue() throws Exception {
        var clock = new ManualScheduler();
        var limiter = limiter(clock, 10, 75);
        var pool = java.util.concurrent.Executors.newFixedThreadPool(16);
        try {
            var start = new java.util.concurrent.CountDownLatch(1);
            var attempts = new ArrayList<java.util.concurrent.Future<Submission>>();
            for (int i = 0; i < 100; i++)
                attempts.add(
                        pool.submit(
                                () -> {
                                    start.await();
                                    return limiter.submit();
                                }));
            start.countDown();
            int allowed = 0, waiting = 0, rejected = 0;
            for (var attempt : attempts) {
                var request = attempt.get(5, java.util.concurrent.TimeUnit.SECONDS);
                if (!request.decision().toCompletableFuture().isDone()) waiting++;
                else if (outcome(request) == Outcome.ALLOWED) allowed++;
                else if (outcome(request) == Outcome.RATE_LIMITED) rejected++;
            }
            assertEquals(10, allowed);
            assertEquals(10, waiting);
            assertEquals(80, rejected);
        } finally {
            limiter.close();
            pool.shutdownNow();
        }
    }

    @Test
    void delayedSchedulerUsesOnlyAccumulatedTokensAndPreservesFifo() {
        var clock = new ManualScheduler();
        var limiter = limiter(clock, 10, 75);
        for (int i = 0; i < 10; i++) limiter.submit();
        var releases = new ArrayList<Integer>();
        var waiting = new ArrayList<Submission>();
        for (int i = 0; i < 10; i++) {
            int index = i;
            waiting.add(
                    limiter.submit(
                            event -> {
                                if (event.decision().equals("ALLOWED")) releases.add(index);
                            }));
        }
        clock.advance(18_000_000_000L);
        assertEquals(java.util.List.of(0, 1, 2), releases);
        for (int i = 0; i < 3; i++) assertEquals(Outcome.ALLOWED, outcome(waiting.get(i)));
        assertFalse(waiting.get(3).decision().toCompletableFuture().isDone());
        limiter.close();
    }

    private Outcome outcome(Submission request) {
        assertTrue(
                request.decision().toCompletableFuture().isDone(), "Expected a settled decision");
        return request.decision().toCompletableFuture().join().outcome();
    }
}
