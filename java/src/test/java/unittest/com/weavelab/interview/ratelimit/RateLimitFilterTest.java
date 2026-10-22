package unittest.com.weavelab.interview.ratelimit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import com.weavelab.interview.ratelimit.RateLimitFilter;
import com.weavelab.interview.ratelimit.service.IRateLimiter;
import com.weavelab.interview.ratelimit.service.impl.LazyDrainingRateLimiter;
import com.weavelab.interview.ratelimit.service.model.RateLimitDecision;

import jakarta.servlet.DispatcherType;
import jakarta.servlet.http.HttpServletResponse;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.slf4j.LoggerFactory;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** Verifies HTTP admission, retry boundaries, exception propagation and safe decision logs. */
class RateLimitFilterTest {
    private final Clock clock = Clock.fixed(Instant.parse("2026-10-21T12:00:00Z"), ZoneOffset.UTC);
    private final Logger logger = (Logger) LoggerFactory.getLogger(RateLimitFilter.class);

    /**
     * Rejects an exhausted bucket without invoking downstream processing. A 1.5-second retry rounds
     * to two seconds, an exact two seconds remains two, and one nanosecond rounds to one. Each
     * response is UTF-8 JSON with status 429 and no-store caching.
     *
     * @param retryNanos remaining drainage duration for the parameterized case
     * @param expectedHeader expected Retry-After value in whole seconds
     */
    @ParameterizedTest
    @CsvSource({"1500000000, 2", "2000000000, 2", "1, 1"})
    void rejectsWithRoundedRetryHeaderAndDoesNotInvokeDownstream(
            long retryNanos, String expectedHeader) throws Exception {
        var limiter = new LazyDrainingRateLimiter(1, Duration.ofNanos(retryNanos), () -> 0L);
        limiter.tryAcquire();
        var filter = new RateLimitFilter(limiter, clock);
        var response = new MockHttpServletResponse();
        var calls = new AtomicInteger();
        filter.doFilter(
                new MockHttpServletRequest("GET", "/health"),
                response,
                (req, res) -> calls.incrementAndGet());
        assertThat(calls).hasValue(0);
        assertThat(response.getStatus()).isEqualTo(429);
        assertThat(response.getHeader("Retry-After")).isEqualTo(expectedHeader);
        assertThat(response.getHeader("Cache-Control")).isEqualTo("no-store");
        assertThat(response.getContentType()).startsWith("application/json");
        assertThat(response.getCharacterEncoding()).isEqualTo("UTF-8");
        assertThat(response.getContentAsString()).isEqualTo("{\"error\":\"Too many requests\"}");
    }

    /**
     * Processes an original request followed by async and error redispatch. All dispatches reach
     * the chain, but only the original consumes allowance and emits a decision log.
     */
    @Test
    void asyncAndErrorRedispatchDoNotConsumeAnotherAdmission() throws Exception {
        var logs = attachLogs();
        try {
            var limiter = new LazyDrainingRateLimiter(2, Duration.ofSeconds(6), () -> 0L);
            var filter = new RateLimitFilter(limiter, clock);
            var calls = new AtomicInteger();
            var request = new MockHttpServletRequest("GET", "/health");
            filter.doFilter(
                    request, new MockHttpServletResponse(), (req, res) -> calls.incrementAndGet());
            request.setDispatcherType(DispatcherType.ASYNC);
            filter.doFilter(
                    request, new MockHttpServletResponse(), (req, res) -> calls.incrementAndGet());
            request.setDispatcherType(DispatcherType.ERROR);
            request.setAttribute("jakarta.servlet.error.request_uri", "/health");
            filter.doFilter(
                    request, new MockHttpServletResponse(), (req, res) -> calls.incrementAndGet());
            assertThat(calls).hasValue(3);
            assertThat(limiter.tryAcquire().allowed()).isTrue();
            assertThat(limiter.tryAcquire().allowed()).isFalse();
            assertThat(decisions(logs)).hasSize(1);
        } finally {
            detachLogs(logs);
        }
    }

    /**
     * Admits a request containing secret authorization, query and body values. Exactly one INFO
     * decision includes request metadata and the atomic debt snapshot while excluding every secret.
     */
    @Test
    void logsRequestTimeAndAtomicStateWithoutSensitiveData() throws Exception {
        var logs = attachLogs();
        try {
            var filter =
                    new RateLimitFilter(
                            new LazyDrainingRateLimiter(1, Duration.ofSeconds(6), () -> 0L), clock);
            var request = new MockHttpServletRequest("POST", "/health");
            request.addHeader("Authorization", "Bearer private-secret");
            request.setQueryString("secret=private-query");
            request.setContent("private-body".getBytes(StandardCharsets.UTF_8));
            filter.doFilter(request, new MockHttpServletResponse(), (req, res) -> {});
            var events = decisions(logs);
            assertThat(events).hasSize(1);
            assertThat(events.get(0).getLevel()).isEqualTo(Level.INFO);
            assertThat(events.get(0).getFormattedMessage())
                    .contains(
                            "requestTime=2026-10-21T12:00:00Z",
                            "decisionTime=",
                            "method=POST",
                            "decision=ALLOWED",
                            "retryAfterMs=0",
                            "debtBeforeDrainNs=0",
                            "debtAfterDrainNs=0",
                            "debtAfterDecisionNs=6000000000",
                            "maxDebtNs=6000000000",
                            "elapsedNs=0")
                    .containsPattern(
                            "requestId=[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
                    .doesNotContain("private-secret", "private-query", "private-body");
        } finally {
            detachLogs(logs);
        }
    }

    /**
     * Makes downstream processing throw IOException after admission. The same exception reaches the
     * caller and the consumed admission is not refunded.
     */
    @Test
    void downstreamFailurePropagatesWithoutRefundingAdmission() {
        var limiter = new LazyDrainingRateLimiter(1, Duration.ofSeconds(6), () -> 0L);
        var filter = new RateLimitFilter(limiter, clock);
        var failure = new IOException("downstream failed");
        assertThatThrownBy(
                        () ->
                                filter.doFilter(
                                        new MockHttpServletRequest("GET", "/health"),
                                        new MockHttpServletResponse(),
                                        (req, res) -> {
                                            throw failure;
                                        }))
                .isSameAs(failure);
        assertThat(limiter.tryAcquire().allowed()).isFalse();
    }

    /**
     * Exhausts the bucket and makes the rejected response writer fail. The same IOException
     * propagates, and the downstream chain is never called.
     */
    @Test
    void responseWritingFailurePropagatesWithoutInvokingDownstream() throws Exception {
        var limiter = new LazyDrainingRateLimiter(1, Duration.ofSeconds(6), () -> 0L);
        limiter.tryAcquire();
        var filter = new RateLimitFilter(limiter, clock);
        var response = mock(HttpServletResponse.class);
        var failure = new IOException("response writer failed");
        when(response.getWriter()).thenThrow(failure);
        var calls = new AtomicInteger();
        assertThatThrownBy(
                        () ->
                                filter.doFilter(
                                        new MockHttpServletRequest("GET", "/health"),
                                        response,
                                        (req, res) -> calls.incrementAndGet()))
                .isSameAs(failure);
        assertThat(calls).hasValue(0);
    }

    /**
     * Retries one second after exhausting a six-second capacity-one bucket. Exactly one INFO
     * rejection records a five-second retry and the precise before/after drainage state.
     */
    @Test
    void rejectedRequestLogsOneInfoDecisionWithExactRetryAndDebt() throws Exception {
        var time = new AtomicLong();
        var limiter = new LazyDrainingRateLimiter(1, Duration.ofSeconds(6), time::get);
        limiter.tryAcquire();
        time.set(Duration.ofSeconds(1).toNanos());
        var logs = attachLogs();
        try {
            var filter = new RateLimitFilter(limiter, clock);
            var calls = new AtomicInteger();
            filter.doFilter(
                    new MockHttpServletRequest("GET", "/health"),
                    new MockHttpServletResponse(),
                    (req, res) -> calls.incrementAndGet());
            var events = decisions(logs);
            assertThat(events).hasSize(1);
            assertThat(events.get(0).getLevel()).isEqualTo(Level.INFO);
            assertThat(events.get(0).getFormattedMessage())
                    .contains(
                            "decision=RATE_LIMITED",
                            "retryAfterMs=5000",
                            "debtBeforeDrainNs=6000000000",
                            "debtAfterDrainNs=5000000000",
                            "debtAfterDecisionNs=5000000000",
                            "maxDebtNs=6000000000",
                            "elapsedNs=1000000000");
            assertThat(calls).hasValue(0);
        } finally {
            detachLogs(logs);
        }
    }

    /** Verifies the filter accepts an interface-only implementation and invokes it once. */
    @Test
    void acceptsLimiterInterfaceWithoutConcreteImplementation() throws Exception {
        var acquisitions = new AtomicInteger();
        IRateLimiter limiter =
                () -> {
                    acquisitions.incrementAndGet();
                    return new RateLimitDecision(
                            true, Duration.ZERO, 0, 0, 1, 1, 0, clock.instant());
                };
        var filter = new RateLimitFilter(limiter, clock);
        var downstream = new AtomicInteger();
        filter.doFilter(
                new MockHttpServletRequest("GET", "/health"),
                new MockHttpServletResponse(),
                (req, res) -> downstream.incrementAndGet());
        assertThat(acquisitions).hasValue(1);
        assertThat(downstream).hasValue(1);
    }

    private ListAppender<ILoggingEvent> attachLogs() {
        var logs = new ListAppender<ILoggingEvent>();
        logs.start();
        logger.addAppender(logs);
        return logs;
    }

    private List<ILoggingEvent> decisions(ListAppender<ILoggingEvent> logs) {
        return logs.list.stream()
                .filter(event -> event.getFormattedMessage().contains("event=rate_limit_decision"))
                .toList();
    }

    private void detachLogs(ListAppender<ILoggingEvent> logs) {
        logger.detachAppender(logs);
        logs.stop();
    }
}
