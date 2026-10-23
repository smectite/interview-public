package unittest.com.weavelab.interview.ratelimit.strategy.impl;

import static org.junit.jupiter.api.Assertions.*;

import com.weavelab.interview.ratelimit.service.ISubmission;
import com.weavelab.interview.ratelimit.service.impl.BoundedQueueRateLimiter;
import com.weavelab.interview.ratelimit.service.model.RateLimitDecision;
import com.weavelab.interview.ratelimit.service.model.RateLimitDecision.Outcome;
import com.weavelab.interview.ratelimit.service.model.RateLimitDecisionEvent;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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

    /**
     * Ten initial tokens admit immediately; ten further requests wait; overflow rejects; one refill
     * releases the first waiter.
     */
    @Test
    void initialTokensAllowTenThenQueueAndRejectOverflow() {
        var clock = new ManualScheduler();
        var limiter = limiter(clock, 10, 75);
        for (int i = 0; i < 10; i++) {
            var request = limiter.submit();
            assertTrue(request.decision().toCompletableFuture().isDone());
            assertEquals(Outcome.ALLOWED, outcome(request));
        }
        var waiting = new ArrayList<ISubmission>();
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

    /**
     * Partial refill time survives a later acquisition; long idle refill cannot exceed token
     * capacity.
     */
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

    /**
     * Submission processes an overdue token for the older waiter before considering the newcomer.
     */
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

    /**
     * Timeout wins at the refill boundary and leaves the earned token available for another
     * request.
     */
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

    /**
     * At a fixed time, 100 callers produce exactly ten admissions, ten waiters and eighty
     * rejections.
     */
    @Test
    void concurrentCallersCannotOverspendTokensOrOverflowQueue() throws Exception {
        var clock = new ManualScheduler();
        var limiter = limiter(clock, 10, 75);
        var pool = Executors.newFixedThreadPool(16);
        try {
            var start = new CountDownLatch(1);
            var attempts = new ArrayList<Future<ISubmission>>();
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
                var request = attempt.get(5, TimeUnit.SECONDS);
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

    /**
     * After eighteen seconds, exactly three earned tokens release FIFO entries zero, one and two.
     */
    @Test
    void delayedSchedulerUsesOnlyAccumulatedTokensAndPreservesFifo() {
        var clock = new ManualScheduler();
        var limiter = limiter(clock, 10, 75);
        for (int i = 0; i < 10; i++) limiter.submit();
        var releases = new ArrayList<Integer>();
        var waiting = new ArrayList<ISubmission>();
        for (int i = 0; i < 10; i++) {
            int index = i;
            waiting.add(
                    limiter.submit(
                            event -> {
                                if (event.decision().equals("ALLOWED")) releases.add(index);
                            }));
        }
        clock.advance(18_000_000_000L);
        assertEquals(List.of(0, 1, 2), releases);
        for (int i = 0; i < 3; i++) assertEquals(Outcome.ALLOWED, outcome(waiting.get(i)));
        assertFalse(waiting.get(3).decision().toCompletableFuture().isDone());
        limiter.close();
    }

    private Outcome outcome(ISubmission request) {
        assertTrue(
                request.decision().toCompletableFuture().isDone(), "Expected a settled decision");
        return request.decision().toCompletableFuture().join().outcome();
    }

    /** Canceling a middle waiter preserves survivor order and does not spend a refill token. */
    @Test
    void cancelMiddleWaiterPreservesFifoAndTokens() {
        var clock = new ManualScheduler();
        var limiter = limiter(clock, 10, 75);
        for (int i = 0; i < 10; i++) limiter.submit();
        var first = limiter.submit();
        var middle = limiter.submit();
        var last = limiter.submit();
        middle.cancel();
        assertEquals(Outcome.CANCELED, outcome(middle));
        clock.advance(6_000_000_000L);
        assertEquals(Outcome.ALLOWED, outcome(first));
        assertFalse(last.decision().toCompletableFuture().isDone());
        clock.advance(6_000_000_000L);
        assertEquals(Outcome.ALLOWED, outcome(last));
        limiter.close();
    }

    /** A new arrival purges an overdue full queue even when the scheduler has not run. */
    @Test
    void arrivalPurgesOverdueQueueBeforeCheckingCapacity() {
        var clock = new ManualScheduler();
        var limiter = limiter(clock, 1, 6);
        limiter.submit();
        var expired = limiter.submit();
        clock.time.set(6_000_000_000L);
        var arrival = limiter.submit();
        assertEquals(Outcome.TIMED_OUT, outcome(expired));
        assertEquals(Outcome.ALLOWED, outcome(arrival));
        limiter.close();
    }

    /** Cancellation and timeout after a grant preserve the grant and cannot refund its token. */
    @Test
    void terminalGrantCannotBeCanceledOrRefunded() {
        var clock = new ManualScheduler();
        var limiter = limiter(clock, 1, 75);
        var granted = limiter.submit();
        granted.cancel();
        granted.timeout();
        assertEquals(Outcome.ALLOWED, outcome(granted));
        assertFalse(limiter.submit().decision().toCompletableFuture().isDone());
        limiter.close();
    }

    /** Completing or canceling a caller-owned future cannot alter internal admission state. */
    @Test
    void decisionViewCannotForgeOrCancelInternalGrant() {
        var clock = new ManualScheduler();
        var limiter = limiter(clock, 1, 75);
        limiter.submit();
        var waiting = limiter.submit();
        var forged = waiting.decision().toCompletableFuture();
        forged.complete(new RateLimitDecision(Outcome.ALLOWED, Duration.ZERO));
        var canceled = waiting.decision().toCompletableFuture();
        canceled.cancel(true);
        assertFalse(waiting.decision().toCompletableFuture().isDone());
        assertEquals(Outcome.RATE_LIMITED, outcome(limiter.submit()));
        clock.advance(6_000_000_000L);
        assertEquals(Outcome.ALLOWED, outcome(waiting));
        assertFalse(limiter.submit().decision().toCompletableFuture().isDone());
        limiter.close();
    }

    /** Queue snapshots contain exact token and deadline state and remain immutable after refill. */
    @Test
    void snapshotsCaptureImmutableTokenAndDeadlineState() {
        var clock = new ManualScheduler();
        var limiter = limiter(clock, 1, 75);
        limiter.submit();
        var events = new ArrayList<RateLimitDecisionEvent>();
        var waiting = limiter.submit(events::add);
        var snapshot = events.get(0).state();
        assertEquals(0L, snapshot.get("queueSizeBefore"));
        assertEquals(1L, snapshot.get("queueSizeAfter"));
        assertEquals(0L, snapshot.get("availableTokens"));
        assertEquals(1L, snapshot.get("tokenCapacity"));
        assertEquals(1L, snapshot.get("waitingCapacity"));
        assertEquals(6_000_000_000L, snapshot.get("nextTokenInNs"));
        assertEquals(75_000_000_000L, snapshot.get("deadlineRemainingNs"));
        assertThrows(
                UnsupportedOperationException.class, () -> snapshot.put("availableTokens", 99L));
        clock.advance(6_000_000_000L);
        assertEquals(Outcome.ALLOWED, outcome(waiting));
        assertEquals(1L, snapshot.get("queueSizeAfter"));
        assertEquals(75_000_000_000L, snapshot.get("deadlineRemainingNs"));
        assertEquals(0L, events.get(1).state().get("queueSizeAfter"));
        limiter.close();
    }
}
