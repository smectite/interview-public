package unittest.com.weavelab.interview.ratelimit.strategy.impl;

import static org.junit.jupiter.api.Assertions.*;

import com.weavelab.interview.ratelimit.service.ISubmission;
import com.weavelab.interview.ratelimit.service.impl.BoundedQueueRateLimiter;
import com.weavelab.interview.ratelimit.service.impl.LazyDrainingRateLimiter;
import com.weavelab.interview.ratelimit.service.impl.SlidingWindowRateLimiter;
import com.weavelab.interview.ratelimit.service.model.RateLimitDecision;
import com.weavelab.interview.ratelimit.service.model.RateLimitDecision.Outcome;
import com.weavelab.interview.ratelimit.service.model.RateLimitDecisionEvent;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/** Deterministic algorithm and lifecycle tests using manually controlled time. */
class RateLimiterCoreTest {
    /**
     * Uses all initial tokens; a waiter expires at the six-second refill/deadline tie and a later
     * request can consume the token.
     */
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

    /**
     * Lazy capacities one and ten admit their bursts, reject overflow and recover at one interval.
     */
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

    /**
     * FIFO waiting is preserved across callbacks; a long delay accumulates tokens that allow a
     * later immediate admission.
     */
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

    /**
     * Capacity-one queue overflow rejects; cancellation frees a waiting slot; deadline expires a
     * waiter; close stops pending work.
     */
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

    /** Concurrent initial admissions never exceed capacities one or ten. */
    @Test
    void concurrentLazyAdmissionsStayWithinCapacity() throws Exception {
        for (int capacity : new int[] {1, 10}) {
            var limiter = new LazyDrainingRateLimiter(capacity, Duration.ofSeconds(6), () -> 0L);
            var pool = Executors.newFixedThreadPool(16);
            try {
                var start = new CountDownLatch(1);
                var attempts = new ArrayList<Future<RateLimitDecision.Outcome>>();
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

    /**
     * Lazy event state cannot be mutated and observer failure does not turn rejection into
     * admission.
     */
    @Test
    void snapshotsAreImmutableAndObserverFailureDoesNotChangeDecision() {
        var limiter = new LazyDrainingRateLimiter(1, Duration.ofSeconds(6), () -> 0L);
        var events = new ArrayList<RateLimitDecisionEvent>();
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

    /**
     * For capacities one and ten, an early scheduler callback cannot release a waiter and overflow
     * does not postpone the refill.
     */
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
            var submissions = new ArrayList<ISubmission>();
            for (int i = 0; i < capacity; i++) submissions.add(limiter.submit());
            assertEquals(RateLimitDecision.Outcome.RATE_LIMITED, outcome(limiter.submit()));
            clock.runEarly();
            assertFalse(submissions.get(0).decision().toCompletableFuture().isDone());
            clock.advance(6_000_000_000L);
            assertEquals(RateLimitDecision.Outcome.ALLOWED, outcome(submissions.get(0)));
            limiter.close();
        }
    }

    /**
     * A reentrant observer advances the scheduler; enqueue still precedes ALLOWED and callbacks run
     * outside the limiter lock.
     */
    @Test
    void queueDeliversEnqueuedBeforeTerminalEvenWithReentrantObserver() {
        var clock = new ManualScheduler();
        var limiter =
                waitingQueue(
                        1, Duration.ofSeconds(6), Duration.ofSeconds(75), clock.time::get, clock);
        var decisions = new ArrayList<String>();
        var submission =
                limiter.submit(
                        event -> {
                            assertFalse(Thread.holdsLock(limiter));
                            if (event.decision().equals("ENQUEUED")) clock.advance(6_000_000_000L);
                            decisions.add(event.decision());
                        });
        assertEquals(List.of("ENQUEUED", "ALLOWED"), decisions);
        assertEquals(RateLimitDecision.Outcome.ALLOWED, outcome(submission));
    }

    /**
     * Explicit timeout, later cancel and later refill settle once; an observer exception cannot
     * leave completion pending.
     */
    @Test
    void explicitTimeoutWinsOnceAndObserverFailureCannotStrandFuture() {
        var clock = new ManualScheduler();
        var limiter =
                waitingQueue(
                        1, Duration.ofSeconds(6), Duration.ofSeconds(75), clock.time::get, clock);
        var events = new ArrayList<String>();
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
        assertEquals(List.of("ENQUEUED", "TIMED_OUT"), events);
    }

    /**
     * Scheduler failure on rescheduling settles both an existing waiter and a new waiter as
     * STOPPED.
     */
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

    /**
     * Across repeated capacity-one races, cancellation or release wins once, events stay ordered
     * and subsequent token availability remains correct.
     */
    @Test
    void concurrentCancellationAndReleaseSettleExactlyOnceAndPreserveSpacing() throws Exception {
        var pool = Executors.newFixedThreadPool(2);
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
                var events = new CopyOnWriteArrayList<RateLimitDecisionEvent>();
                var first = limiter.submit(events::add);
                clock.time.set(6_000_000_000L);
                var start = new CountDownLatch(1);
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
                cancel.get(5, TimeUnit.SECONDS);
                release.get(5, TimeUnit.SECONDS);
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
            assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    /**
     * After depletion, arrivals at one second release the first waiter at six seconds, expire the
     * next at seven, and permit later refill use.
     */
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
            LongSupplier ticker,
            ScheduledExecutorService scheduler) {
        var limiter = new BoundedQueueRateLimiter(capacity, interval, timeout, ticker, scheduler);
        for (int i = 0; i < capacity; i++)
            assertEquals(RateLimitDecision.Outcome.ALLOWED, outcome(limiter.submit()));
        return limiter;
    }

    /** An already shut-down scheduler causes a new waiting submission to settle as STOPPED. */
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

    private static RateLimitDecision.Outcome outcome(ISubmission s) {
        assertTrue(s.decision().toCompletableFuture().isDone(), "Expected settled decision");
        return s.decision().toCompletableFuture().join().outcome();
    }

    /** Fifteen seconds drain two and a half slots; residual debt requires three more seconds. */
    @Test
    void lazyRetainsFractionalDrainAfterMultipleAdmissions() {
        var clock = new ManualScheduler();
        var limiter = new LazyDrainingRateLimiter(10, Duration.ofSeconds(6), clock.time::get);
        for (int i = 0; i < 10; i++) limiter.tryAcquire();
        clock.advance(15_000_000_000L);
        assertEquals(Outcome.ALLOWED, limiter.tryAcquire().outcome());
        assertEquals(Outcome.ALLOWED, limiter.tryAcquire().outcome());
        var events = new ArrayList<RateLimitDecisionEvent>();
        var rejected = limiter.tryAcquire(events::add);
        assertEquals(Outcome.RATE_LIMITED, rejected.outcome());
        assertEquals(Duration.ofSeconds(3), rejected.retryAfter());
        assertEquals(57_000_000_000L, events.get(0).state().get("debtAfterDecisionNs"));
        limiter.close();
    }

    /** When only one slot has drained, one of one hundred simultaneous callers wins admission. */
    @Test
    void concurrentLazyCallersShareSingleRecoveredSlot() throws Exception {
        var clock = new ManualScheduler();
        var limiter = new LazyDrainingRateLimiter(10, Duration.ofSeconds(6), clock.time::get);
        for (int i = 0; i < 10; i++) limiter.tryAcquire();
        clock.advance(6_000_000_000L);
        var pool = Executors.newFixedThreadPool(16);
        try {
            var start = new CountDownLatch(1);
            var attempts = new ArrayList<Future<Outcome>>();
            for (int i = 0; i < 100; i++)
                attempts.add(
                        pool.submit(
                                () -> {
                                    start.await();
                                    return limiter.tryAcquire().outcome();
                                }));
            start.countDown();
            int allowed = 0;
            for (var attempt : attempts)
                if (attempt.get(5, TimeUnit.SECONDS) == Outcome.ALLOWED) allowed++;
            assertEquals(1, allowed);
        } finally {
            pool.shutdownNow();
            limiter.close();
        }
    }

    /**
     * Constructors reject invalid bounds, missing dependencies and nanosecond arithmetic overflow.
     */
    @Test
    void constructorsRejectInvalidBoundsDependenciesAndOverflow() {
        var scheduler = new ManualScheduler();
        Duration second = Duration.ofSeconds(1);
        assertThrows(
                IllegalArgumentException.class,
                () -> new LazyDrainingRateLimiter(0, second, () -> 0L));
        assertThrows(
                IllegalArgumentException.class,
                () -> new LazyDrainingRateLimiter(1, Duration.ZERO, () -> 0L));
        assertThrows(
                NullPointerException.class, () -> new LazyDrainingRateLimiter(1, null, () -> 0L));
        assertThrows(
                NullPointerException.class, () -> new LazyDrainingRateLimiter(1, second, null));
        assertThrows(
                ArithmeticException.class,
                () ->
                        new LazyDrainingRateLimiter(
                                10, Duration.ofNanos(Long.MAX_VALUE / 2), () -> 0L));
        assertThrows(
                ArithmeticException.class,
                () -> new LazyDrainingRateLimiter(1, Duration.ofSeconds(Long.MAX_VALUE), () -> 0L));
        assertThrows(
                IllegalArgumentException.class,
                () -> new SlidingWindowRateLimiter(0, second, () -> 0L));
        assertThrows(
                IllegalArgumentException.class,
                () -> new SlidingWindowRateLimiter(1, Duration.ZERO, () -> 0L));
        assertThrows(
                NullPointerException.class, () -> new SlidingWindowRateLimiter(1, null, () -> 0L));
        assertThrows(
                NullPointerException.class, () -> new SlidingWindowRateLimiter(1, second, null));
        assertThrows(
                ArithmeticException.class,
                () ->
                        new SlidingWindowRateLimiter(
                                1, Duration.ofSeconds(Long.MAX_VALUE), () -> 0L));
        assertThrows(
                IllegalArgumentException.class,
                () -> new BoundedQueueRateLimiter(0, second, second, () -> 0L, scheduler));
        assertThrows(
                IllegalArgumentException.class,
                () -> new BoundedQueueRateLimiter(1, Duration.ZERO, second, () -> 0L, scheduler));
        assertThrows(
                IllegalArgumentException.class,
                () -> new BoundedQueueRateLimiter(1, second, Duration.ZERO, () -> 0L, scheduler));
        assertThrows(
                NullPointerException.class,
                () -> new BoundedQueueRateLimiter(1, second, null, () -> 0L, scheduler));
        assertThrows(
                NullPointerException.class,
                () -> new BoundedQueueRateLimiter(1, second, second, null, scheduler));
        assertThrows(
                NullPointerException.class,
                () -> new BoundedQueueRateLimiter(1, second, second, () -> 0L, null));
        assertThrows(
                ArithmeticException.class,
                () ->
                        new BoundedQueueRateLimiter(
                                10,
                                Duration.ofNanos(Long.MAX_VALUE / 2),
                                second,
                                () -> 0L,
                                scheduler));
        assertThrows(
                ArithmeticException.class,
                () ->
                        new BoundedQueueRateLimiter(
                                1,
                                second,
                                Duration.ofSeconds(Long.MAX_VALUE),
                                () -> 0L,
                                scheduler));
    }
}
