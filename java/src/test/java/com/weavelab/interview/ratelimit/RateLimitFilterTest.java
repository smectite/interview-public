package com.weavelab.interview.ratelimit;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import com.weavelab.interview.config.RateLimitProperties;
import com.weavelab.interview.ratelimit.strategy.*;
import com.weavelab.interview.ratelimit.strategy.model.RateLimitDecision;
import com.weavelab.interview.ratelimit.strategy.model.RateLimitDecisionEvent;

import jakarta.servlet.AsyncContext;
import jakarta.servlet.DispatcherType;

import org.junit.jupiter.api.Test;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/** Verifies HTTP decisions, redispatch, asynchronous races and request-safe logging. */
class RateLimitFilterTest {
    private final Clock clock = Clock.fixed(Instant.parse("2026-10-20T15:00:00Z"), ZoneOffset.UTC);

    @Test
    void rejectedRequestHasRoundedRetryAndNeverInvokesHandler() throws Exception {
        TestImmediateLimiter limiter =
                new TestImmediateLimiter(RateLimitDecision.Outcome.RATE_LIMITED);
        RateLimitFilter filter =
                new ImmediateRateLimitFilter(limiter, new RateLimitProperties(), clock);
        MockHttpServletResponse response = new MockHttpServletResponse();
        AtomicInteger calls = new AtomicInteger();
        filter.doFilter(request(), response, (req, res) -> calls.incrementAndGet());
        assertThat(calls).hasValue(0);
        assertThat(response.getStatus()).isEqualTo(429);
        assertThat(response.getHeader("Retry-After")).isEqualTo("2");
        assertThat(response.getHeader("Cache-Control")).isEqualTo("no-store");
        assertThat(response.getContentType()).startsWith("application/json");
        assertThat(response.getContentAsString()).contains("Too many requests");
    }

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
            listener.onTimeout(new jakarta.servlet.AsyncEvent(async, request, response));
        }
        assertThat(limiter.canceled).isTrue();
        assertThat(response.getStatus()).isEqualTo(503);
        assertThat(async.getDispatchedPath()).isNull();
        limiter.complete(RateLimitDecision.Outcome.ALLOWED);
        assertThat(async.getDispatchedPath()).isNull();
    }

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

    @Test
    void infoLogContainsTimesDecisionAndSnapshotButNotSensitiveRequestData() throws Exception {
        Logger logger = (Logger) LoggerFactory.getLogger(RateLimitFilter.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try {
            TestImmediateLimiter limiter =
                    new TestImmediateLimiter(RateLimitDecision.Outcome.ALLOWED);
            MockHttpServletRequest request = request();
            request.addHeader("Authorization", "Bearer private-secret");
            request.setQueryString("secret=private-query");
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
                            "decision=ALLOWED",
                            "occupancyAfter=1")
                    .doesNotContain("private-secret", "private-query");
        } finally {
            logger.detachAppender(appender);
            appender.stop();
        }
    }

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

    private static final class TestImmediateLimiter implements ImmediateRateLimiter {
        private final RateLimitDecision.Outcome outcome;
        private int submissions;

        TestImmediateLimiter(RateLimitDecision.Outcome outcome) {
            this.outcome = outcome;
        }

        public RateLimitDecision tryAcquire(Consumer<RateLimitDecisionEvent> observer) {
            submissions++;
            observer.accept(
                    new RateLimitDecisionEvent(
                            outcome.name(),
                            "TEST",
                            Duration.ofMillis(1001),
                            Duration.ZERO,
                            Map.of("occupancyBefore", 0L, "occupancyAfter", 1L),
                            Instant.parse("2026-10-20T15:00:00Z")));
            return new RateLimitDecision(outcome, Duration.ofMillis(1001));
        }

        public void close() {}
    }

    private static final class TestLimiter implements QueuedRateLimiter {
        private final CompletableFuture<RateLimitDecision> future = new CompletableFuture<>();
        private final boolean immediate;
        private final RateLimitDecision.Outcome outcome;
        private Consumer<RateLimitDecisionEvent> observer;
        private int submissions;
        private boolean canceled;

        private TestLimiter(boolean immediate, RateLimitDecision.Outcome outcome) {
            this.immediate = immediate;
            this.outcome = outcome;
        }

        @Override
        public Submission submit(Consumer<RateLimitDecisionEvent> observer) {
            this.observer = observer;
            submissions++;
            if (immediate) complete(outcome);
            return new Submission() {
                public java.util.concurrent.CompletionStage<RateLimitDecision> decision() {
                    return future;
                }

                public void cancel() {
                    canceled = true;
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
}
