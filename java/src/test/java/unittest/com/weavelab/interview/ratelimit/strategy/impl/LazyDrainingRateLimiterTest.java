package unittest.com.weavelab.interview.ratelimit.strategy.impl;

import static org.junit.jupiter.api.Assertions.*;

import com.weavelab.interview.ratelimit.strategy.impl.LazyDrainingRateLimiter;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/** Verifies lazy drainage, arbitrary capacities, exact timing and global concurrency. */
class LazyDrainingRateLimiterTest {
    /**
     * Fills the ten-request bucket, rejects before the six-second recovery boundary, and admits
     * exactly one request at that boundary before rejecting again.
     */
    @Test
    void allowsConfiguredBurstThenRecoversAtExactBoundary() {
        var time = new AtomicLong();
        var limiter = new LazyDrainingRateLimiter(10, Duration.ofSeconds(6), time::get);
        for (int i = 0; i < 10; i++) assertTrue(limiter.tryAcquire().allowed());
        assertFalse(limiter.tryAcquire().allowed());
        time.set(5_999_999_999L);
        assertEquals(Duration.ofNanos(1), limiter.tryAcquire().retryAfter());
        time.incrementAndGet();
        assertTrue(limiter.tryAcquire().allowed());
        assertFalse(limiter.tryAcquire().allowed());
    }

    /**
     * Uses capacity three and a 250 ms interval, then advances through a long idle period. The
     * bucket restores exactly three admissions without accumulating extra idle credit.
     */
    @Test
    void supportsArbitraryCapacityAndIntervalWithoutIdleCreditOverflow() {
        var time = new AtomicLong();
        var limiter = new LazyDrainingRateLimiter(3, Duration.ofMillis(250), time::get);
        for (int i = 0; i < 3; i++) assertTrue(limiter.tryAcquire().allowed());
        assertFalse(limiter.tryAcquire().allowed());
        time.addAndGet(Duration.ofHours(1).toNanos());
        for (int i = 0; i < 3; i++) assertTrue(limiter.tryAcquire().allowed());
        assertFalse(limiter.tryAcquire().allowed());
    }

    /**
     * Retries an exhausted capacity-one bucket each second. Rejections preserve the original
     * six-second recovery deadline, and an earlier immutable snapshot remains unchanged.
     */
    @Test
    void rejectedAttemptsDoNotDelayRecoveryAndSnapshotsRemainStable() {
        var time = new AtomicLong();
        var limiter = new LazyDrainingRateLimiter(1, Duration.ofSeconds(6), time::get);
        var first = limiter.tryAcquire();
        for (int i = 1; i < 6; i++) {
            time.set(Duration.ofSeconds(i).toNanos());
            var rejected = limiter.tryAcquire();
            assertFalse(rejected.allowed());
            assertEquals(Duration.ofSeconds(6 - i), rejected.retryAfter());
        }
        time.set(Duration.ofSeconds(6).toNanos());
        assertTrue(limiter.tryAcquire().allowed());
        assertEquals(6_000_000_000L, first.debtAfterDecisionNs());
    }

    /**
     * Starts 100 simultaneous attempts against an unused capacity-ten bucket. Exactly ten callers
     * are admitted, proving the initial check and debt update are atomic.
     */
    @Test
    void concurrentClientsCannotExceedGlobalBurst() throws Exception {
        var limiter = new LazyDrainingRateLimiter(10, Duration.ofSeconds(6), () -> 0L);
        assertEquals(10, countConcurrentAdmissions(limiter));
    }

    /**
     * Exhausts ten admissions and advances fifteen seconds. Exactly two admissions recover; the
     * next request is rejected with 57 seconds of debt and a three-second retry delay.
     */
    @Test
    void partialDrainRestoresOnlyEarnedCapacityAndPreservesRemainingDebt() {
        var time = new AtomicLong();
        var limiter = new LazyDrainingRateLimiter(10, Duration.ofSeconds(6), time::get);
        for (int i = 0; i < 10; i++) assertTrue(limiter.tryAcquire().allowed());
        time.set(Duration.ofSeconds(15).toNanos());
        assertTrue(limiter.tryAcquire().allowed());
        assertTrue(limiter.tryAcquire().allowed());
        var rejected = limiter.tryAcquire();
        assertFalse(rejected.allowed());
        assertEquals(Duration.ofSeconds(3), rejected.retryAfter());
        assertEquals(57_000_000_000L, rejected.debtAfterDecisionNs());
    }

    /**
     * Exhausts the bucket, advances one six-second interval, and races 100 callers. Exactly one
     * receives the recovered admission.
     */
    @Test
    void concurrentCallersCompeteForExactlyOneRecoveredAdmission() throws Exception {
        var time = new AtomicLong();
        var limiter = new LazyDrainingRateLimiter(10, Duration.ofSeconds(6), time::get);
        for (int i = 0; i < 10; i++) assertTrue(limiter.tryAcquire().allowed());
        time.set(Duration.ofSeconds(6).toNanos());
        assertEquals(1, countConcurrentAdmissions(limiter));
    }

    /**
     * Checks empty, partially drained allowed, and rejected transitions. Every debt field, elapsed
     * duration, retry delay and diagnostic timestamp describes the corresponding atomic decision.
     */
    @Test
    void snapshotsDescribeAllowedAndRejectedTransitionsExactly() {
        var time = new AtomicLong();
        var limiter = new LazyDrainingRateLimiter(2, Duration.ofSeconds(6), time::get);
        var first = limiter.tryAcquire();
        assertTrue(first.allowed());
        assertEquals(Duration.ZERO, first.retryAfter());
        assertEquals(0, first.debtBeforeDrainNs());
        assertEquals(0, first.debtAfterDrainNs());
        assertEquals(6_000_000_000L, first.debtAfterDecisionNs());
        assertEquals(12_000_000_000L, first.maxDebtNs());
        assertEquals(0, first.elapsedNs());
        assertNotNull(first.decisionTime());

        time.set(Duration.ofSeconds(3).toNanos());
        var allowed = limiter.tryAcquire();
        assertTrue(allowed.allowed());
        assertEquals(Duration.ZERO, allowed.retryAfter());
        assertEquals(6_000_000_000L, allowed.debtBeforeDrainNs());
        assertEquals(3_000_000_000L, allowed.debtAfterDrainNs());
        assertEquals(9_000_000_000L, allowed.debtAfterDecisionNs());
        assertEquals(12_000_000_000L, allowed.maxDebtNs());
        assertEquals(3_000_000_000L, allowed.elapsedNs());
        assertNotNull(allowed.decisionTime());

        time.set(Duration.ofSeconds(4).toNanos());
        var rejected = limiter.tryAcquire();
        assertFalse(rejected.allowed());
        assertEquals(Duration.ofSeconds(2), rejected.retryAfter());
        assertEquals(9_000_000_000L, rejected.debtBeforeDrainNs());
        assertEquals(8_000_000_000L, rejected.debtAfterDrainNs());
        assertEquals(8_000_000_000L, rejected.debtAfterDecisionNs());
        assertEquals(12_000_000_000L, rejected.maxDebtNs());
        assertEquals(1_000_000_000L, rejected.elapsedNs());
        assertNotNull(rejected.decisionTime());
    }

    /**
     * Exhausts one limiter and verifies a second limiter still admits its own request.
     * Instance-local debt must not be shared through static state.
     */
    @Test
    void separateLimiterInstancesDoNotShareDebt() {
        var first = new LazyDrainingRateLimiter(1, Duration.ofSeconds(6), () -> 0L);
        var second = new LazyDrainingRateLimiter(1, Duration.ofSeconds(6), () -> 0L);
        assertTrue(first.tryAcquire().allowed());
        assertFalse(first.tryAcquire().allowed());
        assertTrue(second.tryAcquire().allowed());
        assertFalse(first.tryAcquire().allowed());
        assertFalse(second.tryAcquire().allowed());
    }

    private int countConcurrentAdmissions(LazyDrainingRateLimiter limiter) throws Exception {
        var pool = Executors.newFixedThreadPool(16);
        try {
            var start = new CountDownLatch(1);
            var results = new java.util.ArrayList<Future<Boolean>>();
            for (int i = 0; i < 100; i++)
                results.add(
                        pool.submit(
                                () -> {
                                    start.await();
                                    return limiter.tryAcquire().allowed();
                                }));
            start.countDown();
            int allowed = 0;
            for (var result : results) if (result.get(5, TimeUnit.SECONDS)) allowed++;
            return allowed;
        } finally {
            pool.shutdownNow();
            assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    /**
     * Rejects zero capacity or interval, missing interval or ticker, maximum-debt overflow, and
     * duration-to-nanoseconds overflow. Conversion overflow retains its arithmetic cause.
     */
    @Test
    void invalidConstructionFailsClearly() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new LazyDrainingRateLimiter(0, Duration.ofSeconds(1), () -> 0L));
        assertThrows(
                IllegalArgumentException.class,
                () -> new LazyDrainingRateLimiter(1, Duration.ZERO, () -> 0L));
        assertThrows(
                IllegalArgumentException.class,
                () -> new LazyDrainingRateLimiter(2, Duration.ofNanos(Long.MAX_VALUE), () -> 0L));
        assertThrows(
                IllegalArgumentException.class,
                () -> new LazyDrainingRateLimiter(1, null, () -> 0L));
        assertThrows(
                IllegalArgumentException.class,
                () -> new LazyDrainingRateLimiter(1, Duration.ofSeconds(6), null));
        var conversionOverflow =
                assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                new LazyDrainingRateLimiter(
                                        1, Duration.ofSeconds(Long.MAX_VALUE), () -> 0L));
        assertInstanceOf(ArithmeticException.class, conversionOverflow.getCause());
    }
}
