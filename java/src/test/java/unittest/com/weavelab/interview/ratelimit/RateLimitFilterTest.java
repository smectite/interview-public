package unittest.com.weavelab.interview.ratelimit;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import com.weavelab.interview.config.RateLimitProperties;
import com.weavelab.interview.ratelimit.ImmediateRateLimitFilter;
import com.weavelab.interview.ratelimit.QueuedRateLimitFilter;
import com.weavelab.interview.ratelimit.RateLimitFilter;
import com.weavelab.interview.ratelimit.service.*;
import com.weavelab.interview.ratelimit.service.impl.LazyDrainingRateLimiter;
import com.weavelab.interview.ratelimit.service.model.RateLimitDecision;
import com.weavelab.interview.ratelimit.service.model.RateLimitDecisionEvent;

import jakarta.servlet.AsyncContext;
import jakarta.servlet.AsyncEvent;
import jakarta.servlet.DispatcherType;
import jakarta.servlet.ServletException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.slf4j.LoggerFactory;
import org.springframework.mock.web.MockAsyncContext;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/** Verifies HTTP decisions, redispatch, asynchronous races and request-safe logging. */
class RateLimitFilterTest {
    private final Clock clock = Clock.fixed(Instant.parse("2026-10-20T15:00:00Z"), ZoneOffset.UTC);

    /**
     * Rejected requests never invoke the handler; one nanosecond rounds to one second, 1001
     * milliseconds to two, and exact two seconds stays two. Responses include 429, JSON encoded as
     * UTF-8 and no-store.
     */
    @ParameterizedTest
    @CsvSource({"1,1", "1001000000,2", "2000000000,2"})
    void rejectedRequestHasRoundedRetryAndNeverInvokesHandler(long nanos, String expected)
            throws Exception {
        TestImmediateLimiter limiter =
                new TestImmediateLimiter(RateLimitDecision.Outcome.RATE_LIMITED);
        limiter.retry = Duration.ofNanos(nanos);
        RateLimitFilter filter =
                new ImmediateRateLimitFilter(limiter, new RateLimitProperties(), clock);
        MockHttpServletResponse response = new MockHttpServletResponse();
        AtomicInteger calls = new AtomicInteger();
        filter.doFilter(request(), response, (req, res) -> calls.incrementAndGet());
        assertThat(calls).hasValue(0);
        assertThat(response.getStatus()).isEqualTo(429);
        assertThat(response.getHeader("Retry-After")).isEqualTo(expected);
        assertThat(response.getCharacterEncoding()).isEqualTo("UTF-8");
        assertThat(response.getHeader("Cache-Control")).isEqualTo("no-store");
        assertThat(response.getContentType()).startsWith("application/json");
        assertThat(response.getContentAsString()).contains("Too many requests");
    }

    /**
     * Pending request starts async, dispatches only after grant, and does not acquire again on
     * async redispatch.
     */
    @Test
    void pendingRequestDispatchesOnlyAfterReleaseWithoutReacquiring() throws Exception {
        TestLimiter limiter = new TestLimiter(false, RateLimitDecision.Outcome.ALLOWED);
        RateLimitFilter filter =
                new QueuedRateLimitFilter(limiter, new RateLimitProperties(), clock);
        MockHttpServletRequest request = request();
        MockHttpServletResponse response = new MockHttpServletResponse();
        AtomicInteger calls = new AtomicInteger();
        filter.doFilter(request, response, (req, res) -> calls.incrementAndGet());
        assertThat(request.isAsyncStarted()).isTrue();
        assertThat(calls).hasValue(0);
        MockAsyncContext async = (MockAsyncContext) request.getAsyncContext();
        limiter.complete(RateLimitDecision.Outcome.ALLOWED);
        assertThat(async.getDispatchedPath()).isEqualTo("/health");
        request.setDispatcherType(DispatcherType.ASYNC);
        filter.doFilter(request, response, (req, res) -> calls.incrementAndGet());
        assertThat(calls).hasValue(1);
        assertThat(limiter.submissions).isEqualTo(1);
    }

    /**
     * Completion during startAsync is handled without losing the grant, executing the handler on
     * the completion thread or submitting twice.
     */
    @Test
    void releaseDuringAsyncSetupStillDispatchesExactlyOnce() throws Exception {
        TestLimiter limiter = new TestLimiter(false, RateLimitDecision.Outcome.ALLOWED);
        MockHttpServletRequest request =
                new MockHttpServletRequest("GET", "/health") {
                    @Override
                    public AsyncContext startAsync() {
                        AsyncContext context = super.startAsync();
                        limiter.complete(RateLimitDecision.Outcome.ALLOWED);
                        return context;
                    }
                };
        request.setAsyncSupported(true);
        new QueuedRateLimitFilter(limiter, new RateLimitProperties(), clock)
                .doFilter(
                        request,
                        new MockHttpServletResponse(),
                        (req, res) -> {
                            throw new AssertionError(
                                    "Must dispatch, not run handler on completion thread");
                        });
        assertThat(((MockAsyncContext) request.getAsyncContext()).getDispatchedPath())
                .isEqualTo("/health");
        assertThat(limiter.submissions).isEqualTo(1);
    }

    /** Servlet timeout cancels pending work, returns 503, and ignores a subsequent grant. */
    @Test
    void asyncTimeoutCancelsPendingSubmissionAndReturnsUnavailable() throws Exception {
        TestLimiter limiter = new TestLimiter(false, RateLimitDecision.Outcome.ALLOWED);
        RateLimitFilter filter =
                new QueuedRateLimitFilter(limiter, new RateLimitProperties(), clock);
        MockHttpServletRequest request = request();
        MockHttpServletResponse response = new MockHttpServletResponse();
        filter.doFilter(
                request,
                response,
                (req, res) -> {
                    throw new AssertionError("pending");
                });
        MockAsyncContext async = (MockAsyncContext) request.getAsyncContext();
        for (var listener : async.getListeners()) {
            listener.onTimeout(new AsyncEvent(async, request, response));
        }
        assertThat(limiter.canceled).isTrue();
        assertThat(response.getStatus()).isEqualTo(503);
        assertThat(async.getDispatchedPath()).isNull();
        limiter.complete(RateLimitDecision.Outcome.ALLOWED);
        assertThat(async.getDispatchedPath()).isNull();
    }

    /**
     * Completed immediate grant invokes the chain synchronously; error redispatch does not acquire
     * again.
     */
    @Test
    void completedAllowCanBeHandledSynchronouslyAndErrorRedispatchIsNotCharged() throws Exception {
        TestImmediateLimiter limiter = new TestImmediateLimiter(RateLimitDecision.Outcome.ALLOWED);
        RateLimitFilter filter =
                new ImmediateRateLimitFilter(limiter, new RateLimitProperties(), clock);
        MockHttpServletRequest request = request();
        MockHttpServletResponse response = new MockHttpServletResponse();
        AtomicInteger calls = new AtomicInteger();
        filter.doFilter(request, response, (req, res) -> calls.incrementAndGet());
        assertThat(request.isAsyncStarted()).isFalse();
        request.setDispatcherType(DispatcherType.ERROR);
        filter.doFilter(request, response, (req, res) -> calls.incrementAndGet());
        assertThat(calls).hasValue(2);
        assertThat(limiter.submissions).isEqualTo(1);
    }

    /**
     * Allowed and rejected requests log INFO decisions with times, configuration and state while
     * excluding authorization, query and request-body secrets.
     */
    @ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(
            value = RateLimitDecision.Outcome.class,
            names = {"ALLOWED", "RATE_LIMITED"})
    void infoLogContainsTimesDecisionAndSnapshotButNotSensitiveRequestData(
            RateLimitDecision.Outcome outcome) throws Exception {
        Logger logger = (Logger) LoggerFactory.getLogger(RateLimitFilter.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try {
            TestImmediateLimiter limiter = new TestImmediateLimiter(outcome);
            MockHttpServletRequest request = request();
            request.addHeader("Authorization", "Bearer private-secret");
            request.setQueryString("secret=private-query");
            request.setContent(
                    "private-body-secret".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            new ImmediateRateLimitFilter(limiter, new RateLimitProperties(), clock)
                    .doFilter(request, new MockHttpServletResponse(), (req, res) -> {});
            assertThat(appender.list).hasSize(1);
            ILoggingEvent event = appender.list.get(0);
            assertThat(event.getLevel()).isEqualTo(Level.INFO);
            assertThat(event.getFormattedMessage())
                    .contains(
                            "event=rate_limit_decision",
                            "requestTime=2026-10-20T15:00:00Z",
                            "decisionTime=2026-10-20T15:00:00Z",
                            "approach=LAZY_DRAINING",
                            "capacity=10",
                            "decision=" + outcome,
                            "occupancyAfter=1")
                    .doesNotContain("private-secret", "private-query", "private-body-secret");
        } finally {
            logger.detachAppender(appender);
            appender.stop();
        }
    }

    /** Immediate processing needs no servlet async support and submits once. */
    @Test
    void immediateFilterWorksWithoutServletAsyncSupport() throws Exception {
        var limiter = new TestImmediateLimiter(RateLimitDecision.Outcome.ALLOWED);
        var filter = new ImmediateRateLimitFilter(limiter, new RateLimitProperties(), clock);
        var request = request();
        request.setAsyncSupported(false);
        var response = new MockHttpServletResponse();
        var calls = new AtomicInteger();
        filter.doFilter(request, response, (req, res) -> calls.incrementAndGet());
        assertThat(calls).hasValue(1);
        assertThat(request.isAsyncStarted()).isFalse();
        assertThat(limiter.submissions).isEqualTo(1);
    }

    /**
     * A future already failed before async setup is canceled and returns 503 without starting
     * async.
     */
    @Test
    void exceptionalQueuedDecisionCancelsAndReturnsUnavailableWithoutStartingAsync()
            throws Exception {
        var limiter = new TestLimiter(false, RateLimitDecision.Outcome.ALLOWED);
        limiter.future.completeExceptionally(new IllegalStateException("queue unavailable"));
        var filter = new QueuedRateLimitFilter(limiter, new RateLimitProperties(), clock);
        var request = request();
        var response = new MockHttpServletResponse();
        filter.doFilter(
                request,
                response,
                (req, res) -> {
                    throw new AssertionError();
                });
        assertThat(response.getStatus()).isEqualTo(503);
        assertThat(limiter.canceled).isTrue();
        assertThat(request.isAsyncStarted()).isFalse();
    }

    private MockHttpServletRequest request() {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/health");
        request.setAsyncSupported(true);
        return request;
    }

    private static final class TestImmediateLimiter implements IImmediateRateLimiter {
        private final RateLimitDecision.Outcome outcome;
        private int submissions;
        private Duration retry = Duration.ofMillis(1001);

        TestImmediateLimiter(RateLimitDecision.Outcome outcome) {
            this.outcome = outcome;
        }

        public RateLimitDecision tryAcquire(Consumer<RateLimitDecisionEvent> observer) {
            submissions++;
            observer.accept(
                    new RateLimitDecisionEvent(
                            outcome.name(),
                            "TEST",
                            retry,
                            Duration.ZERO,
                            Map.of("occupancyBefore", 0L, "occupancyAfter", 1L),
                            Instant.parse("2026-10-20T15:00:00Z")));
            return new RateLimitDecision(outcome, retry);
        }

        public void close() {}
    }

    private static final class TestLimiter implements IQueuedRateLimiter {
        private final CompletableFuture<RateLimitDecision> future = new CompletableFuture<>();
        private final boolean immediate;
        private final RateLimitDecision.Outcome outcome;
        private Consumer<RateLimitDecisionEvent> observer;
        private int submissions;
        private boolean canceled;
        private int cancellations;

        private TestLimiter(boolean immediate, RateLimitDecision.Outcome outcome) {
            this.immediate = immediate;
            this.outcome = outcome;
        }

        @Override
        public ISubmission submit(Consumer<RateLimitDecisionEvent> observer) {
            this.observer = observer;
            submissions++;
            if (immediate) complete(outcome);
            return new ISubmission() {
                public CompletionStage<RateLimitDecision> decision() {
                    return future;
                }

                public void cancel() {
                    canceled = true;
                    cancellations++;
                }
            };
        }

        void complete(RateLimitDecision.Outcome outcome) {
            observer.accept(
                    new RateLimitDecisionEvent(
                            outcome.name(),
                            "TEST",
                            Duration.ofMillis(1001),
                            Duration.ZERO,
                            Map.of("occupancyBefore", 0L, "occupancyAfter", 1L),
                            Instant.parse("2026-10-20T15:00:00Z")));
            future.complete(new RateLimitDecision(outcome, Duration.ofMillis(1001)));
        }

        public void close() {}
    }

    /**
     * Pending admission without servlet async support is canceled and returns 503 without a handler
     * call.
     */
    @Test
    void pendingRequestWithoutAsyncSupportIsCanceled() throws Exception {
        var limiter = new TestLimiter(false, RateLimitDecision.Outcome.ALLOWED);
        var request = request();
        request.setAsyncSupported(false);
        var response = new MockHttpServletResponse();
        new QueuedRateLimitFilter(limiter, new RateLimitProperties(), clock)
                .doFilter(
                        request,
                        response,
                        (req, res) -> {
                            throw new AssertionError("Not admitted");
                        });
        assertThat(response.getStatus()).isEqualTo(503);
        assertThat(limiter.cancellations).isEqualTo(1);
        assertThat(request.isAsyncStarted()).isFalse();
    }

    /**
     * A startAsync failure retains its throwable in a warning and cancels the pending admission.
     */
    @Test
    void asyncSetupFailureIsLoggedAndCanceled() throws Exception {
        var limiter = new TestLimiter(false, RateLimitDecision.Outcome.ALLOWED);
        var failure = new IllegalStateException("container unavailable");
        var request =
                new MockHttpServletRequest("GET", "/health") {
                    @Override
                    public AsyncContext startAsync() {
                        throw failure;
                    }
                };
        request.setAsyncSupported(true);
        var response = new MockHttpServletResponse();
        Logger logger = (Logger) LoggerFactory.getLogger(QueuedRateLimitFilter.class);
        var logs = new ListAppender<ILoggingEvent>();
        logs.start();
        logger.addAppender(logs);
        try {
            new QueuedRateLimitFilter(limiter, new RateLimitProperties(), clock)
                    .doFilter(
                            request,
                            response,
                            (req, res) -> {
                                throw new AssertionError("Not admitted");
                            });
            assertThat(response.getStatus()).isEqualTo(503);
            assertThat(limiter.cancellations).isEqualTo(1);
            assertThat(logs.list)
                    .anySatisfy(
                            event -> {
                                assertThat(event.getLevel()).isEqualTo(Level.WARN);
                                assertThat(event.getThrowableProxy().getMessage())
                                        .isEqualTo(failure.getMessage());
                            });
        } finally {
            logger.detachAppender(logs);
            logs.stop();
        }
    }

    /** Failure after async setup logs the cause, completes a 503 response and never dispatches. */
    @Test
    void pendingDecisionFailureCompletesUnavailableResponse() throws Exception {
        var limiter = new TestLimiter(false, RateLimitDecision.Outcome.ALLOWED);
        var request = request();
        var response = new MockHttpServletResponse();
        Logger logger = (Logger) LoggerFactory.getLogger(QueuedRateLimitFilter.class);
        var logs = new ListAppender<ILoggingEvent>();
        logs.start();
        logger.addAppender(logs);
        try {
            new QueuedRateLimitFilter(limiter, new RateLimitProperties(), clock)
                    .doFilter(
                            request,
                            response,
                            (req, res) -> {
                                throw new AssertionError("Not admitted");
                            });
            var async = (MockAsyncContext) request.getAsyncContext();
            limiter.future.completeExceptionally(new IllegalStateException("pending failure"));
            assertThat(response.getStatus()).isEqualTo(503);
            assertThat(request.isAsyncStarted()).isFalse();
            assertThat(async.getDispatchedPath()).isNull();
            assertThat(logs.list)
                    .anySatisfy(
                            event -> {
                                assertThat(event.getLevel()).isEqualTo(Level.ERROR);
                                assertThat(event.getThrowableProxy()).isNotNull();
                                assertThat(event.getThrowableProxy().getMessage())
                                        .contains("pending failure");
                            });
        } finally {
            logger.detachAppender(logs);
            logs.stop();
        }
    }

    /**
     * A normal STOPPED queue result completes async processing with 503 rather than dispatching.
     */
    @Test
    void stoppedPendingDecisionCompletesUnavailableResponse() throws Exception {
        var limiter = new TestLimiter(false, RateLimitDecision.Outcome.ALLOWED);
        var request = request();
        var response = new MockHttpServletResponse();
        new QueuedRateLimitFilter(limiter, new RateLimitProperties(), clock)
                .doFilter(
                        request,
                        response,
                        (req, res) -> {
                            throw new AssertionError("Not admitted");
                        });
        var async = (MockAsyncContext) request.getAsyncContext();
        limiter.complete(RateLimitDecision.Outcome.STOPPED);
        assertThat(response.getStatus()).isEqualTo(503);
        assertThat(request.isAsyncStarted()).isFalse();
        assertThat(async.getDispatchedPath()).isNull();
    }

    /**
     * Completion before a grant cancels exactly once and prevents a late grant from dispatching.
     */
    @Test
    void earlyServletCompletionCancelsOnceAndIgnoresLateGrant() throws Exception {
        var limiter = new TestLimiter(false, RateLimitDecision.Outcome.ALLOWED);
        var request = request();
        var response = new MockHttpServletResponse();
        new QueuedRateLimitFilter(limiter, new RateLimitProperties(), clock)
                .doFilter(
                        request,
                        response,
                        (req, res) -> {
                            throw new AssertionError("Not admitted");
                        });
        var async = (MockAsyncContext) request.getAsyncContext();
        for (var listener : async.getListeners()) {
            listener.onComplete(new AsyncEvent(async));
            listener.onComplete(new AsyncEvent(async));
        }
        limiter.complete(RateLimitDecision.Outcome.ALLOWED);
        assertThat(limiter.cancellations).isEqualTo(1);
        assertThat(async.getDispatchedPath()).isNull();
    }

    /**
     * An ungranted async redispatch is rejected without another admission attempt or handler call.
     */
    @Test
    void ungrantedAsyncRedispatchIsRejectedWithoutAcquisition() throws Exception {
        var limiter = new TestImmediateLimiter(RateLimitDecision.Outcome.ALLOWED);
        var request = request();
        request.setDispatcherType(DispatcherType.ASYNC);
        var response = new MockHttpServletResponse();
        new ImmediateRateLimitFilter(limiter, new RateLimitProperties(), clock)
                .doFilter(
                        request,
                        response,
                        (req, res) -> {
                            throw new AssertionError("No grant");
                        });
        assertThat(response.getStatus()).isEqualTo(503);
        assertThat(limiter.submissions).isZero();
    }

    /**
     * A downstream exception propagates unchanged and the admitted request still consumes capacity.
     */
    @Test
    void downstreamFailureDoesNotRefundAdmission() throws Exception {
        var limiter = new LazyDrainingRateLimiter(1, Duration.ofSeconds(6), () -> 0L);
        var filter = new ImmediateRateLimitFilter(limiter, new RateLimitProperties(), clock);
        var failure = new ServletException("handler failed");
        Assertions.assertSame(
                failure,
                Assertions.assertThrows(
                        ServletException.class,
                        () ->
                                filter.doFilter(
                                        request(),
                                        new MockHttpServletResponse(),
                                        (req, res) -> {
                                            throw failure;
                                        })));
        var response = new MockHttpServletResponse();
        filter.doFilter(
                request(),
                response,
                (req, res) -> {
                    throw new AssertionError("Quota spent");
                });
        assertThat(response.getStatus()).isEqualTo(429);
        limiter.close();
    }
}
