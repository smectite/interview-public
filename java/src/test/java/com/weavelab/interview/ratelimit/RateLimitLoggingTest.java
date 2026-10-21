package com.weavelab.interview.ratelimit;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import com.weavelab.interview.config.RateLimitProperties;
import com.weavelab.interview.ratelimit.strategy.QueuedRateLimiter;
import com.weavelab.interview.ratelimit.strategy.impl.BoundedQueueRateLimiter;

import jakarta.servlet.AsyncEvent;

import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.mock.web.MockAsyncContext;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

/** Verifies ordered queue transition logs retain original request metadata. */
class RateLimitLoggingTest {
    @Test
    void queueLogsEveryTransitionWithOriginalRequestTimeAndNoRepeatedAdmission() throws Exception {
        Logger logger = (Logger) LoggerFactory.getLogger(RateLimitFilter.class);
        ListAppender<ILoggingEvent> logs = new ListAppender<>();
        logs.start();
        logger.addAppender(logs);
        ManualScheduler scheduler = new ManualScheduler();
        RateLimitProperties properties = new RateLimitProperties();
        properties.setApproach(RateLimitProperties.Approach.BOUNDED_QUEUE);
        try (QueuedRateLimiter limiter =
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
}
