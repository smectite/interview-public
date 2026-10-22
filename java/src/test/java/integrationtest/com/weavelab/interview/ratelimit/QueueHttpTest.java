package integrationtest.com.weavelab.interview.ratelimit;

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
import com.weavelab.interview.config.RateLimitProperties;
import com.weavelab.interview.config.WebConfig;
import com.weavelab.interview.ratelimit.RateLimitFilter;
import com.weavelab.interview.ratelimit.service.IQueuedRateLimiter;
import com.weavelab.interview.ratelimit.service.ISubmission;
import com.weavelab.interview.ratelimit.service.impl.BoundedQueueRateLimiter;

import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import unittest.com.weavelab.interview.ratelimit.strategy.impl.ManualScheduler;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

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

    /**
     * Real servlet requests cover health 200, authenticated 200, unauthorized 401 and controller-
     * error 500, with enqueue/allow logs proving queue participation and no repeated admission.
     */
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
            var auth = authFuture.get(5, TimeUnit.SECONDS);
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

    /**
     * A genuinely queued HTTP request fills the only waiting slot; overflow returns 429 before
     * refill.
     */
    @Test
    void fullQueueRejectsHttpOverflowBeforeRelease() throws Exception {
        try (var context = controlledServer("30s")) {
            var harness = context.getBean(ControlledQueue.class);
            int serverPort =
                    ((org.springframework.boot.web.servlet.context
                                            .ServletWebServerApplicationContext)
                                    context)
                            .getWebServer()
                            .getPort();
            var client = HttpClient.newHttpClient();
            assertThat(
                            client.send(
                                            controlledRequest(serverPort),
                                            HttpResponse.BodyHandlers.ofString())
                                    .statusCode())
                    .isEqualTo(200);
            var waiting =
                    client.sendAsync(
                            controlledRequest(serverPort), HttpResponse.BodyHandlers.ofString());
            harness.awaitEnqueued();
            var overflow =
                    client.send(
                            controlledRequest(serverPort), HttpResponse.BodyHandlers.ofString());
            assertThat(overflow.statusCode()).isEqualTo(429);
            assertThat(overflow.headers().firstValue("Retry-After")).hasValue("6");
            assertThat(overflow.headers().firstValue("Cache-Control")).hasValue("no-store");
            assertThat(waiting.isDone()).isFalse();
            harness.scheduler.advance(6_000_000_000L);
            assertThat(waiting.get(5, TimeUnit.SECONDS).statusCode()).isEqualTo(200);
        }
    }

    /**
     * The real servlet timeout returns 503 and frees the waiting slot for a later successful
     * request.
     */
    @Test
    void servletTimeoutFreesQueueCapacityForNextHttpRequest() throws Exception {
        try (var context = controlledServer("1s")) {
            var harness = context.getBean(ControlledQueue.class);
            int serverPort =
                    ((org.springframework.boot.web.servlet.context
                                            .ServletWebServerApplicationContext)
                                    context)
                            .getWebServer()
                            .getPort();
            var client = HttpClient.newHttpClient();
            assertThat(
                            client.send(
                                            controlledRequest(serverPort),
                                            HttpResponse.BodyHandlers.ofString())
                                    .statusCode())
                    .isEqualTo(200);
            var overdue =
                    client.sendAsync(
                            controlledRequest(serverPort), HttpResponse.BodyHandlers.ofString());
            harness.awaitEnqueued();
            var timeout = overdue.get(5, TimeUnit.SECONDS);
            assertThat(timeout.statusCode()).isEqualTo(503);
            assertThat(timeout.body()).contains("timeout");
            harness.scheduler.advance(5_500_000_000L);
            var replacement =
                    client.sendAsync(
                            controlledRequest(serverPort), HttpResponse.BodyHandlers.ofString());
            harness.awaitEnqueued();
            assertThat(replacement.isDone()).isFalse();
            // Only the algorithm's clock advances; servlet timeout remains real container behavior.
            harness.scheduler.advance(500_000_000L);
            assertThat(replacement.get(5, TimeUnit.SECONDS).statusCode()).isEqualTo(200);
        }
    }

    private ConfigurableApplicationContext controlledServer(String timeout) {
        return new SpringApplicationBuilder(ControlledApplication.class)
                .profiles("rate-limit-http-test")
                .run(
                        "--server.port=0",
                        "--app.rate-limit.approach=BOUNDED_QUEUE",
                        "--app.rate-limit.capacity=1",
                        "--app.rate-limit.queue-timeout=" + timeout);
    }

    private HttpRequest controlledRequest(int serverPort) {
        return HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + serverPort + "/health"))
                .timeout(Duration.ofSeconds(5))
                .build();
    }

    /** Supplies a test-owned monotonic clock while retaining the actual servlet/filter pipeline. */
    @Configuration(proxyBeanMethods = false)
    @Import(TestApplication.class)
    static class ControlledApplication {
        @Bean
        @Primary
        ControlledQueue controlledQueue(RateLimitProperties properties) {
            return new ControlledQueue(properties.getQueueTimeout());
        }
    }

    /**
     * Controls token availability and exposes enqueue coordination without replacing queue
     * behavior.
     */
    static final class ControlledQueue implements IQueuedRateLimiter {
        final ManualScheduler scheduler = new ManualScheduler();
        final BoundedQueueRateLimiter delegate;
        final BlockingQueue<Boolean> enqueued = new LinkedBlockingQueue<>();

        ControlledQueue(Duration timeout) {
            delegate =
                    new BoundedQueueRateLimiter(
                            1, Duration.ofSeconds(6), timeout, scheduler.time::get, scheduler);
        }

        @Override
        public ISubmission submit(
                Consumer<com.weavelab.interview.ratelimit.service.model.RateLimitDecisionEvent>
                        observer) {
            return delegate.submit(
                    event -> {
                        observer.accept(event);
                        if (event.decision().equals("ENQUEUED")) enqueued.add(true);
                    });
        }

        void awaitEnqueued() throws InterruptedException {
            assertThat(enqueued.poll(5, TimeUnit.SECONDS))
                    .as("Request must enter the real bounded queue")
                    .isTrue();
        }

        @Override
        public void close() {
            delegate.close();
            scheduler.shutdownNow();
        }
    }

    private HttpRequest.Builder request(String path) {
        return HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .timeout(Duration.ofSeconds(5));
    }

    @Profile("rate-limit-http-test")
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
