package unittest.com.weavelab.interview.ratelimit.strategy.impl;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import com.weavelab.interview.ratelimit.service.impl.BoundedQueueRateLimiter;
import com.weavelab.interview.ratelimit.service.impl.LazyDrainingRateLimiter;
import com.weavelab.interview.ratelimit.service.model.RateLimitDecision;

import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/** Verifies invocation logging, retained exception evidence and failure-safe admission. */
class RateLimitDiagnosticsTest {
    /**
     * Constructor, acquisition and close log fixed method names at INFO without argument values.
     */
    @Test
    void publicInvocationsAreLoggedWithoutArguments() {
        Logger logger = (Logger) LoggerFactory.getLogger(LazyDrainingRateLimiter.class);
        var appender = new ListAppender<ILoggingEvent>();
        appender.start();
        logger.addAppender(appender);
        try {
            var limiter = new LazyDrainingRateLimiter(10, Duration.ofSeconds(6), () -> 0L);
            limiter.tryAcquire();
            limiter.close();
            assertThat(appender.list)
                    .extracting(ILoggingEvent::getFormattedMessage)
                    .contains(
                            "event=method_invocation method=LazyDrainingRateLimiter.<init>",
                            "event=method_invocation method=LazyDrainingRateLimiter.tryAcquire",
                            "event=method_invocation method=LazyDrainingRateLimiter.close");
            assertThat(appender.list)
                    .allSatisfy(event -> assertThat(event.getLevel()).isEqualTo(Level.INFO));
        } finally {
            logger.detachAppender(appender);
            appender.stop();
        }
    }

    /**
     * An observer exception produces WARN with a throwable while preserving an allowed decision.
     */
    @Test
    void observerFailureIsLoggedWithThrowableAndAdmissionStillSucceeds() {
        Logger logger =
                (Logger)
                        LoggerFactory.getLogger(
                                "com.weavelab.interview.ratelimit.strategy.impl.LimiterSupport");
        var appender = new ListAppender<ILoggingEvent>();
        appender.start();
        logger.addAppender(appender);
        try {
            var limiter = new LazyDrainingRateLimiter(10, Duration.ofSeconds(6), () -> 0L);
            var decision =
                    limiter.tryAcquire(
                            event -> {
                                throw new IllegalStateException("observer failed");
                            });
            assertThat(decision.outcome()).isEqualTo(RateLimitDecision.Outcome.ALLOWED);
            assertThat(appender.list)
                    .anySatisfy(
                            event -> {
                                assertThat(event.getLevel()).isEqualTo(Level.WARN);
                                assertThat(event.getThrowableProxy()).isNotNull();
                                assertThat(event.getFormattedMessage()).contains("observer");
                            });
        } finally {
            logger.detachAppender(appender);
            appender.stop();
        }
    }

    /**
     * Scheduler rejection emits ERROR with a throwable outside the state lock and settles pending
     * work as STOPPED.
     */
    @Test
    void rejectedSchedulerIsLoggedOutsideStateLockAndSettlesQueue() {
        Logger logger = (Logger) LoggerFactory.getLogger(BoundedQueueRateLimiter.class);
        var clock = new ManualScheduler();
        var limiter =
                new BoundedQueueRateLimiter(
                        1, Duration.ofSeconds(6), Duration.ofSeconds(6), clock.time::get, clock);
        var appender =
                new ListAppender<ILoggingEvent>() {
                    @Override
                    protected void append(ILoggingEvent event) {
                        if (event.getLevel() == Level.ERROR)
                            assertThat(Thread.holdsLock(limiter)).isFalse();
                        super.append(event);
                    }
                };
        appender.start();
        logger.addAppender(appender);
        try {
            limiter.submit();
            clock.shutdown();
            assertThat(limiter.submit().decision().toCompletableFuture().join().outcome())
                    .isEqualTo(RateLimitDecision.Outcome.STOPPED);
            assertThat(appender.list)
                    .anySatisfy(
                            event -> {
                                assertThat(event.getLevel()).isEqualTo(Level.ERROR);
                                assertThat(event.getThrowableProxy()).isNotNull();
                            });
        } finally {
            logger.detachAppender(appender);
            appender.stop();
        }
    }

    /** Submission handles log fixed invocation names at INFO without including caller arguments. */
    @Test
    void queueHandleInvocationsAreLoggedWithoutArguments() {
        Logger logger = (Logger) LoggerFactory.getLogger(BoundedQueueRateLimiter.class);
        var logs = new ListAppender<ILoggingEvent>();
        logs.start();
        logger.addAppender(logs);
        var clock = new ManualScheduler();
        try (var limiter =
                new BoundedQueueRateLimiter(
                        1, Duration.ofSeconds(6), Duration.ofSeconds(75), clock.time::get, clock)) {
            limiter.submit();
            var canceled = limiter.submit();
            canceled.decision();
            canceled.cancel();
            var timedOut = limiter.submit();
            timedOut.timeout();
            assertThat(logs.list)
                    .filteredOn(event -> event.getFormattedMessage().contains(".Entry."))
                    .allSatisfy(
                            event -> {
                                assertThat(event.getLevel()).isEqualTo(Level.INFO);
                                assertThat(event.getFormattedMessage())
                                        .matches(
                                                "event=method_invocation"
                                                    + " method=BoundedQueueRateLimiter.Entry.(decision|cancel|timeout)");
                            });
            assertThat(logs.list)
                    .extracting(ILoggingEvent::getFormattedMessage)
                    .contains(
                            "event=method_invocation method=BoundedQueueRateLimiter.Entry.decision",
                            "event=method_invocation method=BoundedQueueRateLimiter.Entry.cancel",
                            "event=method_invocation method=BoundedQueueRateLimiter.Entry.timeout");
            assertThat(canceled.decision().toCompletableFuture().join().outcome())
                    .isEqualTo(RateLimitDecision.Outcome.CANCELED);
            assertThat(timedOut.decision().toCompletableFuture().join().outcome())
                    .isEqualTo(RateLimitDecision.Outcome.TIMED_OUT);
        } finally {
            logger.detachAppender(logs);
            logs.stop();
        }
    }
}
