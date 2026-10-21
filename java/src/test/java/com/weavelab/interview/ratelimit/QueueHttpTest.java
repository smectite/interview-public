package com.weavelab.interview.ratelimit;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import com.weavelab.interview.api.AuthController;
import com.weavelab.interview.api.HealthController;
import com.weavelab.interview.auth.AuthFilter;
import com.weavelab.interview.auth.AuthInterceptor;
import com.weavelab.interview.config.FilterConfig;
import com.weavelab.interview.config.RateLimitConfig;
import com.weavelab.interview.config.WebConfig;

import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/** Exercises real servlet asynchronous dispatch and authentication through the queue adapter. */
@ActiveProfiles("rate-limit-http-test")
@SpringBootTest(
        classes = QueueHttpTest.TestApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
            "app.rate-limit.approach=BOUNDED_QUEUE",
            "app.rate-limit.capacity=1",
            "app.rate-limit.requests=1",
            "app.rate-limit.period=500ms",
            "app.rate-limit.queue-timeout=2s"
        })
class QueueHttpTest {
    @LocalServerPort private int port;

    @Test
    void realContainerResumesQueuedHealthAuthenticationAndErrorRequests() throws Exception {
        Logger logger = (Logger) LoggerFactory.getLogger(RateLimitFilter.class);
        ListAppender<ILoggingEvent> decisions = new ListAppender<>();
        decisions.start();
        logger.addAppender(decisions);
        try {
            HttpClient client =
                    HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
            var health =
                    client.send(request("/health").build(), HttpResponse.BodyHandlers.ofString());
            assertThat(health.statusCode()).isEqualTo(200);
            assertThat(health.body()).isEqualTo("ok");

            var authFuture =
                    client.sendAsync(
                            request("/api/auth/token")
                                    .header("Authorization", "Bearer alice@example.test")
                                    .POST(HttpRequest.BodyPublishers.noBody())
                                    .build(),
                            HttpResponse.BodyHandlers.ofString());
            var auth = authFuture.get(5, java.util.concurrent.TimeUnit.SECONDS);
            assertThat(auth.statusCode()).isEqualTo(200);
            assertThat(auth.body()).contains("alice@example.test", "authenticated");

            var unauthenticated =
                    client.send(
                            request("/api/auth/token")
                                    .POST(HttpRequest.BodyPublishers.noBody())
                                    .build(),
                            HttpResponse.BodyHandlers.ofString());
            assertThat(unauthenticated.statusCode()).isEqualTo(401);

            var error = client.send(request("/boom").build(), HttpResponse.BodyHandlers.ofString());
            assertThat(error.statusCode()).isEqualTo(500);
            assertThat(error.body()).doesNotContain("Too many requests");

            // Successful responses alone also occur if the rate filter is accidentally omitted.
            var messages = decisions.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
            long enqueued =
                    messages.stream()
                            .filter(message -> message.contains("decision=ENQUEUED "))
                            .count();
            assertThat(enqueued).isBetween(1L, 3L);
            assertThat(messages).hasSize(4 + (int) enqueued);
            assertThat(
                            messages.stream()
                                    .filter(message -> message.contains("decision=ENQUEUED "))
                                    .count())
                    .isEqualTo(enqueued);
            assertThat(
                            messages.stream()
                                    .filter(message -> message.contains("decision=ALLOWED "))
                                    .count())
                    .isEqualTo(4);
            assertThat(messages)
                    .allSatisfy(message -> assertThat(message).contains("approach=BOUNDED_QUEUE"));
        } finally {
            logger.detachAppender(decisions);
            decisions.stop();
        }
    }

    private HttpRequest.Builder request(String path) {
        return HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .timeout(Duration.ofSeconds(5));
    }

    @org.springframework.context.annotation.Profile("rate-limit-http-test")
    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration(exclude = DataSourceAutoConfiguration.class)
    @Import({
        RateLimitConfig.class,
        FilterConfig.class,
        AuthFilter.class,
        AuthInterceptor.class,
        WebConfig.class,
        HealthController.class,
        AuthController.class,
        FailureEndpoint.class
    })
    static class TestApplication {}

    @RestController
    static class FailureEndpoint {
        @GetMapping("/boom")
        String fail() {
            throw new IllegalStateException("Expected test failure");
        }
    }
}
