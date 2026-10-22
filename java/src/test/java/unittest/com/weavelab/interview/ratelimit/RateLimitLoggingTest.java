package unittest.com.weavelab.interview.ratelimit;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import com.weavelab.interview.config.RateLimitProperties;
import com.weavelab.interview.ratelimit.QueuedRateLimitFilter;
import com.weavelab.interview.ratelimit.RateLimitFilter;
import com.weavelab.interview.ratelimit.service.IQueuedRateLimiter;
import com.weavelab.interview.ratelimit.service.impl.BoundedQueueRateLimiter;

import jakarta.servlet.AsyncEvent;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;
import org.springframework.mock.web.MockAsyncContext;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import unittest.com.weavelab.interview.ratelimit.strategy.impl.ManualScheduler;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashSet;

/** Verifies ordered queue transition logs retain original request metadata. */
class RateLimitLoggingTest {
    /**
     * Queue logs enqueue plus ALLOWED, RATE_LIMITED, TIMED_OUT, CANCELED and STOPPED transitions;
     * INFO severity, state keys, original request identity/time and release wait are checked.
     */
    @Test
    void queueLogsEveryTransitionWithOriginalRequestTimeAndNoRepeatedAdmission() throws Exception {
        Logger logger = (Logger) LoggerFactory.getLogger(RateLimitFilter.class);
        ListAppender<ILoggingEvent> logs = new ListAppender<>();
        logs.start();
        logger.addAppender(logs);
        ManualScheduler scheduler = new ManualScheduler();
        RateLimitProperties properties = new RateLimitProperties();
        properties.setApproach(RateLimitProperties.Approach.BOUNDED_QUEUE);
        try (IQueuedRateLimiter limiter =
                new BoundedQueueRateLimiter(
                        1,
                        Duration.ofSeconds(6),
                        Duration.ofSeconds(75),
                        scheduler.time::get,
                        scheduler)) {
            limiter.submit(); // Consume the immediate idle release before observing queued
            // requests.
            RateLimitFilter filter =
                    new QueuedRateLimitFilter(
                            limiter,
                            properties,
                            Clock.fixed(Instant.parse("2026-10-20T15:00:00Z"), ZoneOffset.UTC));
            MockHttpServletRequest first = submit(filter);
            MockHttpServletRequest full = submit(filter);
            assertThat(full.isAsyncStarted()).isFalse();
            scheduler.advance(6_000_000_000L);
            assertThat(((MockAsyncContext) first.getAsyncContext()).getDispatchedPath())
                    .isEqualTo("/health");
            MockHttpServletRequest timedOut = submit(filter);
            MockAsyncContext timeoutContext = (MockAsyncContext) timedOut.getAsyncContext();
            for (var listener : timeoutContext.getListeners())
                listener.onTimeout(new AsyncEvent(timeoutContext));
            MockHttpServletRequest disconnected = submit(filter);
            MockAsyncContext disconnectContext = (MockAsyncContext) disconnected.getAsyncContext();
            for (var listener : disconnectContext.getListeners())
                listener.onError(new AsyncEvent(disconnectContext));
            submit(filter);
            limiter.close();
            var messages = logs.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
            assertThat(messages).hasSize(9); // Four enqueue events, five terminal decisions.
            for (String outcome :
                    new String[] {"RATE_LIMITED", "ALLOWED", "TIMED_OUT", "CANCELED", "STOPPED"}) {
                assertThat(
                                messages.stream()
                                        .filter(m -> m.contains("decision=" + outcome + " "))
                                        .count())
                        .isEqualTo(1);
            }
            assertThat(logs.list)
                    .allSatisfy(event -> assertThat(event.getLevel()).isEqualTo(Level.INFO));
            assertThat(messages)
                    .allSatisfy(
                            message ->
                                    assertThat(message)
                                            .contains(
                                                    "requestTime=2026-10-20T15:00:00Z",
                                                    "decisionTime=",
                                                    "queueSizeBefore=",
                                                    "queueSizeAfter="));
            String requestId = messages.get(0).split("requestId=")[1].split(" ")[0];
            assertThat(messages.get(2)).contains("requestId=" + requestId, "waitedMs=6000");
        } finally {
            logger.detachAppender(logs);
            logs.stop();
        }
    }

    private MockHttpServletRequest submit(RateLimitFilter filter) throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/health");
        request.setAsyncSupported(true);
        filter.doFilter(
                request,
                new MockHttpServletResponse(),
                (req, res) -> {
                    throw new AssertionError(
                            "Queued requests must dispatch instead of invoking this chain");
                });
        return request;
    }

    /**
     * Overlapping requests retain distinct IDs, arrival times and ordered per-request state
     * snapshots.
     */
    @Test
    void overlappingQueueLogsKeepRequestMetadataIndependent() throws Exception {
        Logger logger = (Logger) LoggerFactory.getLogger(RateLimitFilter.class);
        var logs = new ListAppender<ILoggingEvent>();
        logs.start();
        logger.addAppender(logs);
        var scheduler = new ManualScheduler();
        var properties = new RateLimitProperties();
        properties.setApproach(RateLimitProperties.Approach.BOUNDED_QUEUE);
        properties.setQueueTimeout(Duration.ofSeconds(75));
        var wallClock = Mockito.mock(Clock.class);
        var firstTime = Instant.parse("2026-10-20T15:00:00Z");
        Mockito.when(wallClock.instant())
                .thenReturn(firstTime, firstTime.plusSeconds(1), firstTime.plusSeconds(2));
        try (var limiter =
                new BoundedQueueRateLimiter(
                        10,
                        Duration.ofSeconds(6),
                        Duration.ofSeconds(75),
                        scheduler.time::get,
                        scheduler)) {
            for (int i = 0; i < 10; i++) limiter.submit();
            var filter = new QueuedRateLimitFilter(limiter, properties, wallClock);
            for (int i = 0; i < 3; i++) submit(filter);
            for (int i = 0; i < 3; i++) scheduler.advance(6_000_000_000L);
            var messages = logs.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
            assertThat(messages).hasSize(6);
            var ids = new HashSet<String>();
            for (int i = 0; i < 3; i++) {
                var enqueued = messages.get(i);
                var released = messages.get(i + 3);
                var id = enqueued.split("requestId=")[1].split(" ")[0];
                assertThat(ids.add(id)).isTrue();
                assertThat(enqueued)
                        .contains(
                                "decision=ENQUEUED ",
                                "requestTime=" + firstTime.plusSeconds(i),
                                "queueSizeBefore=" + i,
                                "queueSizeAfter=" + (i + 1));
                assertThat(released)
                        .contains(
                                "requestId=" + id,
                                "requestTime=" + firstTime.plusSeconds(i),
                                "decision=ALLOWED ",
                                "waitedMs=" + ((i + 1) * 6000));
            }
        } finally {
            logger.detachAppender(logs);
            logs.stop();
        }
    }
}
