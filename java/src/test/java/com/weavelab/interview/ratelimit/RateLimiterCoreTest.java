package com.weavelab.interview.ratelimit;

import static org.junit.jupiter.api.Assertions.*;

import com.weavelab.interview.ratelimit.strategy.Submission;
import com.weavelab.interview.ratelimit.strategy.impl.BoundedQueueRateLimiter;
import com.weavelab.interview.ratelimit.strategy.impl.LazyDrainingRateLimiter;
import com.weavelab.interview.ratelimit.strategy.model.RateLimitDecision;
import com.weavelab.interview.ratelimit.strategy.model.RateLimitDecisionEvent;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/** Deterministic algorithm and lifecycle tests using manually controlled time. */
class RateLimiterCoreTest {
    @Test
    void idleQueueReleasesImmediatelyThenExpiresAtSixSecondDeadline() {
        var clock = new ManualScheduler();
        var limiter =
                new BoundedQueueRateLimiter(
                        10, Duration.ofSeconds(6), Duration.ofSeconds(6), clock.time::get, clock);
        for (int i = 0; i < 9; i++)
            assertEquals(RateLimitDecision.Outcome.ALLOWED, outcome(limiter.submit()));
        var first = limiter.submit();
        assertTrue(first.decision().toCompletableFuture().isDone());
        assertEquals(RateLimitDecision.Outcome.ALLOWED, outcome(first));
        var waiting = limiter.submit();
        assertFalse(waiting.decision().toCompletableFuture().isDone());
        clock.advance(6_000_000_000L);
        assertEquals(RateLimitDecision.Outcome.TIMED_OUT, outcome(waiting));
        assertEquals(RateLimitDecision.Outcome.ALLOWED, outcome(limiter.submit()));
    }

    @Test
    void lazyHonorsCapacityAndExactBoundary() {
        var time = new AtomicLong();
        for (int capacity : new int[] {1, 10}) {
            var limiter = new LazyDrainingRateLimiter(capacity, Duration.ofSeconds(6), time::get);
            for (int i = 0; i < capacity; i++)
                assertEquals(RateLimitDecision.Outcome.ALLOWED, limiter.tryAcquire().outcome());
            assertEquals(RateLimitDecision.Outcome.RATE_LIMITED, limiter.tryAcquire().outcome());
            time.addAndGet(Duration.ofSeconds(6).toNanos());
            assertEquals(RateLimitDecision.Outcome.ALLOWED, limiter.tryAcquire().outcome());
        }
    }

    @Test
    void queueIsFifoAndUsesAccumulatedTokensAfterDelayedCallbacks() {
        var clock = new ManualScheduler();
        var limiter =
                waitingQueue(
                        10, Duration.ofSeconds(6), Duration.ofSeconds(75), clock.time::get, clock);
        var first = limiter.submit();
        var second = limiter.submit();
        assertFalse(first.decision().toCompletableFuture().isDone());
        clock.advance(6_000_000_000L);
        assertEquals(RateLimitDecision.Outcome.ALLOWED, outcome(first));
        assertFalse(second.decision().toCompletableFuture().isDone());
        clock.advance(60_000_000_000L);
        assertEquals(RateLimitDecision.Outcome.ALLOWED, outcome(second));
        var third = limiter.submit();
        assertTrue(third.decision().toCompletableFuture().isDone());
        assertEquals(RateLimitDecision.Outcome.ALLOWED, outcome(third));
    }

    @Test
    void queueExpiresAtDeadlineAndCancellationFreesSlot() {
        var clock = new ManualScheduler();
        var limiter =
                waitingQueue(
                        1, Duration.ofSeconds(6), Duration.ofSeconds(2), clock.time::get, clock);
        var first = limiter.submit();
        assertEquals(RateLimitDecision.Outcome.RATE_LIMITED, outcome(limiter.submit()));
        first.cancel();
        assertEquals(RateLimitDecision.Outcome.CANCELED, outcome(first));
        var second = limiter.submit();
        clock.advance(2_000_000_000L);
        assertEquals(RateLimitDecision.Outcome.TIMED_OUT, outcome(second));
        var third = limiter.submit();
        limiter.close();
        assertEquals(RateLimitDecision.Outcome.STOPPED, outcome(third));
    }

    @Test
    void concurrentLazyAdmissionsStayWithinCapacity() throws Exception {
        for (int capacity : new int[] {1, 10}) {
            var limiter = new LazyDrainingRateLimiter(capacity, Duration.ofSeconds(6), () -> 0L);
            var pool = java.util.concurrent.Executors.newFixedThreadPool(16);
            try {
                var start = new java.util.concurrent.CountDownLatch(1);
                var attempts =
                        new java.util.ArrayList<
                                java.util.concurrent.Future<RateLimitDecision.Outcome>>();
                for (int i = 0; i < 64; i++)
                    attempts.add(
                            pool.submit(
                                    () -> {
                                        start.await();
                                        return outcome(limiter.tryAcquire());
                                    }));
                start.countDown();
                int allowed = 0;
                for (var attempt : attempts)
                    if (attempt.get() == RateLimitDecision.Outcome.ALLOWED) allowed++;
                assertEquals(capacity, allowed);
            } finally {
                pool.shutdownNow();
            }
        }
    }

    @Test
    void snapshotsAreImmutableAndObserverFailureDoesNotChangeDecision() {
        var limiter = new LazyDrainingRateLimiter(1, Duration.ofSeconds(6), () -> 0L);
        var events = new java.util.ArrayList<RateLimitDecisionEvent>();
        assertEquals(RateLimitDecision.Outcome.ALLOWED, outcome(limiter.tryAcquire(events::add)));
        assertThrows(UnsupportedOperationException.class, () -> events.get(0).state().put("x", 1L));
        assertEquals(
                RateLimitDecision.Outcome.RATE_LIMITED,
                outcome(
                        limiter.tryAcquire(
                                e -> {
                                    throw new IllegalStateException();
                                })));
        assertEquals(6_000_000_000L, events.get(0).state().get("debtAfterDecisionNs"));
    }

    @Test
    void queueEarlyCallbackCannotReleaseAndOverflowDoesNotDelay() {
        for (int capacity : new int[] {1, 10}) {
            var clock = new ManualScheduler();
            var limiter =
                    waitingQueue(
                            capacity,
                            Duration.ofSeconds(6),
                            Duration.ofSeconds(75),
                            clock.time::get,
                            clock);
            var submissions = new java.util.ArrayList<Submission>();
            for (int i = 0; i < capacity; i++) submissions.add(limiter.submit());
            assertEquals(RateLimitDecision.Outcome.RATE_LIMITED, outcome(limiter.submit()));
            clock.runEarly();
            assertFalse(submissions.get(0).decision().toCompletableFuture().isDone());
            clock.advance(6_000_000_000L);
            assertEquals(RateLimitDecision.Outcome.ALLOWED, outcome(submissions.get(0)));
            limiter.close();
        }
    }

    @Test
    void queueDeliversEnqueuedBeforeTerminalEvenWithReentrantObserver() {
        var clock = new ManualScheduler();
        var limiter =
                waitingQueue(
                        1, Duration.ofSeconds(6), Duration.ofSeconds(75), clock.time::get, clock);
        var decisions = new java.util.ArrayList<String>();
        var submission =
                limiter.submit(
                        event -> {
                            assertFalse(Thread.holdsLock(limiter));
                            if (event.decision().equals("ENQUEUED")) clock.advance(6_000_000_000L);
                            decisions.add(event.decision());
                        });
        assertEquals(java.util.List.of("ENQUEUED", "ALLOWED"), decisions);
        assertEquals(RateLimitDecision.Outcome.ALLOWED, outcome(submission));
    }

    @Test
    void explicitTimeoutWinsOnceAndObserverFailureCannotStrandFuture() {
        var clock = new ManualScheduler();
        var limiter =
                waitingQueue(
                        1, Duration.ofSeconds(6), Duration.ofSeconds(75), clock.time::get, clock);
        var events = new java.util.ArrayList<String>();
        var submission =
                limiter.submit(
                        event -> {
                            events.add(event.decision());
                            if (!event.decision().equals("ENQUEUED"))
                                throw new IllegalStateException("broken logger");
                        });
        submission.timeout();
        submission.cancel();
        clock.advance(6_000_000_000L);
        assertEquals(RateLimitDecision.Outcome.TIMED_OUT, outcome(submission));
        assertEquals(java.util.List.of("ENQUEUED", "TIMED_OUT"), events);
    }

    @Test
    void schedulerFailureWhileReschedulingSettlesExistingQueue() {
        var clock = new ManualScheduler();
        var limiter =
                waitingQueue(
                        10, Duration.ofSeconds(6), Duration.ofSeconds(75), clock.time::get, clock);
        var first = limiter.submit();
        clock.shutdown();
        var second = limiter.submit();
        assertEquals(RateLimitDecision.Outcome.STOPPED, outcome(first));
        assertEquals(RateLimitDecision.Outcome.STOPPED, outcome(second));
    }

    @Test
    void concurrentCancellationAndReleaseSettleExactlyOnceAndPreserveSpacing() throws Exception {
        var pool = java.util.concurrent.Executors.newFixedThreadPool(2);
        try {
            for (int attempt = 0; attempt < 100; attempt++) {
                var clock = new ManualScheduler();
                var limiter =
                        waitingQueue(
                                1,
                                Duration.ofSeconds(6),
                                Duration.ofSeconds(75),
                                clock.time::get,
                                clock);
                var events =
                        new java.util.concurrent.CopyOnWriteArrayList<RateLimitDecisionEvent>();
                var first = limiter.submit(events::add);
                clock.time.set(6_000_000_000L);
                var start = new java.util.concurrent.CountDownLatch(1);
                var cancel =
                        pool.submit(
                                () -> {
                                    start.await();
                                    first.cancel();
                                    return null;
                                });
                var release =
                        pool.submit(
                                () -> {
                                    start.await();
                                    clock.runDue();
                                    return null;
                                });
                start.countDown();
                cancel.get(5, java.util.concurrent.TimeUnit.SECONDS);
                release.get(5, java.util.concurrent.TimeUnit.SECONDS);
                var result = outcome(first);
                assertTrue(
                        result == RateLimitDecision.Outcome.ALLOWED
                                || result == RateLimitDecision.Outcome.CANCELED);
                assertEquals(2, events.size());
                assertEquals("ENQUEUED", events.get(0).decision());
                assertEquals(result.name(), events.get(1).decision());
                var next = limiter.submit();
                clock.runDue();
                if (result == RateLimitDecision.Outcome.ALLOWED) {
                    assertFalse(next.decision().toCompletableFuture().isDone());
                    clock.advance(6_000_000_000L);
                }
                assertEquals(RateLimitDecision.Outcome.ALLOWED, outcome(next));
                limiter.close();
            }
        } finally {
            pool.shutdownNow();
            assertTrue(pool.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS));
        }
    }

    @Test
    void sixSecondQueueReleasesBeforeDeadlineAndExpiresExcessWaitingRequests() {
        var clock = new ManualScheduler();
        var limiter =
                new BoundedQueueRateLimiter(
                        10, Duration.ofSeconds(6), Duration.ofSeconds(6), clock.time::get, clock);
        for (int i = 0; i < 10; i++)
            assertEquals(RateLimitDecision.Outcome.ALLOWED, outcome(limiter.submit()));
        clock.advance(1_000_000_000L);
        var firstWaiting = limiter.submit();
        var secondWaiting = limiter.submit();
        clock.advance(5_000_000_000L);
        assertEquals(RateLimitDecision.Outcome.ALLOWED, outcome(firstWaiting));
        assertFalse(secondWaiting.decision().toCompletableFuture().isDone());
        clock.advance(1_000_000_000L);
        assertEquals(RateLimitDecision.Outcome.TIMED_OUT, outcome(secondWaiting));
        clock.advance(5_000_000_000L);
        assertEquals(RateLimitDecision.Outcome.ALLOWED, outcome(limiter.submit()));
    }

    private static BoundedQueueRateLimiter waitingQueue(
            int capacity,
            Duration interval,
            Duration timeout,
            java.util.function.LongSupplier ticker,
            java.util.concurrent.ScheduledExecutorService scheduler) {
        var limiter = new BoundedQueueRateLimiter(capacity, interval, timeout, ticker, scheduler);
        for (int i = 0; i < capacity; i++)
            assertEquals(RateLimitDecision.Outcome.ALLOWED, outcome(limiter.submit()));
        return limiter;
    }

    @Test
    void schedulerRejectionStopsPendingQueue() {
        var clock = new ManualScheduler();
        var limiter =
                waitingQueue(
                        10, Duration.ofSeconds(6), Duration.ofSeconds(6), clock.time::get, clock);
        clock.shutdown();
        assertEquals(RateLimitDecision.Outcome.STOPPED, outcome(limiter.submit()));
    }

    private static RateLimitDecision.Outcome outcome(RateLimitDecision decision) {
        return decision.outcome();
    }

    private static RateLimitDecision.Outcome outcome(Submission s) {
        assertTrue(s.decision().toCompletableFuture().isDone(), "Expected settled decision");
        return s.decision().toCompletableFuture().join().outcome();
    }
}
