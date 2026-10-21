package integrationtest.com.weavelab.interview.ratelimit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.weavelab.interview.api.AuthController;
import com.weavelab.interview.api.HealthController;
import com.weavelab.interview.auth.AuthFilter;
import com.weavelab.interview.auth.AuthInterceptor;
import com.weavelab.interview.config.RateLimitProperties;
import com.weavelab.interview.config.WebConfig;
import com.weavelab.interview.ratelimit.RateLimitFilter;
import com.weavelab.interview.ratelimit.strategy.impl.LazyDrainingRateLimiter;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;
import org.springframework.core.annotation.Order;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.net.http.*;

/** Exercises automatic component registration, annotation order and the shared HTTP quota. */
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@ActiveProfiles("lazy-rate-limit-http-test")
@SpringBootTest(
        classes = RateLimitHttpTest.TestApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
            "app.rate-limit.capacity=3",
            "app.rate-limit.requests=1",
            "app.rate-limit.period=60s"
        })
class RateLimitHttpTest {
    @LocalServerPort private int port;
    @SpyBean private AuthFilter authFilter;

    /**
     * Runs real HTTP requests through annotation-registered filters. Health, authenticated and
     * unauthenticated requests from different clients share three admissions; exhaustion returns
     * 429 with retry and cache headers.
     */
    @Test
    void annotationsRegisterGlobalLimitBeforeAuthentication() throws Exception {
        assertThat(RateLimitFilter.class.getAnnotation(Order.class).value()).isEqualTo(0);
        assertThat(AuthFilter.class.getAnnotation(Order.class).value()).isEqualTo(1);
        var alice = HttpClient.newHttpClient();
        var bob = HttpClient.newHttpClient();
        assertThat(
                        alice.send(request("/health").build(), HttpResponse.BodyHandlers.ofString())
                                .statusCode())
                .isEqualTo(200);
        assertThat(
                        bob.send(
                                        request("/api/auth/token")
                                                .POST(HttpRequest.BodyPublishers.noBody())
                                                .build(),
                                        HttpResponse.BodyHandlers.ofString())
                                .statusCode())
                .isEqualTo(401);
        assertThat(
                        alice.send(
                                        request("/api/auth/token")
                                                .header(
                                                        "Authorization",
                                                        "Bearer alice@example.test")
                                                .POST(HttpRequest.BodyPublishers.noBody())
                                                .build(),
                                        HttpResponse.BodyHandlers.ofString())
                                .statusCode())
                .isEqualTo(200);
        var exhausted =
                bob.send(
                        request("/api/auth/token")
                                .POST(HttpRequest.BodyPublishers.noBody())
                                .build(),
                        HttpResponse.BodyHandlers.ofString());
        assertThat(exhausted.statusCode()).isEqualTo(429);
        assertThat(exhausted.headers().firstValue("Retry-After"))
                .hasValueSatisfying(value -> assertThat(Integer.parseInt(value)).isBetween(1, 60));
        assertThat(exhausted.headers().firstValue("Cache-Control")).hasValue("no-store");
    }

    /**
     * Exhausts the shared quota through real HTTP requests, then observes the authentication filter
     * directly. A rejected request returns 429 without invoking authentication, proving runtime
     * filter order.
     */
    @Test
    void rejectedRequestsNeverEnterAuthenticationFilter() throws Exception {
        var client = HttpClient.newHttpClient();
        for (int i = 0; i < 3; i++) {
            var allowed =
                    client.send(request("/health").build(), HttpResponse.BodyHandlers.ofString());
            assertThat(allowed.statusCode()).isEqualTo(200);
        }
        verify(authFilter, times(3)).doFilter(any(), any(), any());
        clearInvocations(authFilter);
        var rejected =
                client.send(
                        request("/api/auth/token")
                                .POST(HttpRequest.BodyPublishers.noBody())
                                .build(),
                        HttpResponse.BodyHandlers.ofString());
        assertThat(rejected.statusCode()).isEqualTo(429);
        verify(authFilter, never()).doFilter(any(), any(), any());
    }

    /**
     * Triggers an unhandled controller exception and verifies the container error response. Two
     * further requests remain eligible in a capacity-three bucket, then the next is rejected: the
     * failure consumes one admission without a refund or redispatch charge.
     */
    @Test
    void realContainerErrorDispatchConsumesOnlyTheOriginalAdmission() throws Exception {
        var client = HttpClient.newHttpClient();
        var error =
                client.send(
                        request("/rate-limit-test/fail").build(),
                        HttpResponse.BodyHandlers.ofString());
        assertThat(error.statusCode()).isEqualTo(500);
        var errorBody = new com.fasterxml.jackson.databind.ObjectMapper().readTree(error.body());
        assertThat(errorBody.path("status").asInt()).isEqualTo(500);
        assertThat(errorBody.path("path").asText()).isEqualTo("/rate-limit-test/fail");
        for (int i = 0; i < 2; i++) {
            var allowed =
                    client.send(request("/health").build(), HttpResponse.BodyHandlers.ofString());
            assertThat(allowed.statusCode()).isEqualTo(200);
        }
        var exhausted =
                client.send(request("/health").build(), HttpResponse.BodyHandlers.ofString());
        assertThat(exhausted.statusCode()).isEqualTo(429);
    }

    private HttpRequest.Builder request(String path) {
        return HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .timeout(java.time.Duration.ofSeconds(5));
    }

    /** Isolates the real servlet filter chain from database startup checks. */
    @Configuration(proxyBeanMethods = false)
    @Profile("lazy-rate-limit-http-test")
    @EnableAutoConfiguration(exclude = DataSourceAutoConfiguration.class)
    @Import({
        RateLimitProperties.class,
        LazyDrainingRateLimiter.class,
        RateLimitFilter.class,
        AuthFilter.class,
        AuthInterceptor.class,
        WebConfig.class,
        HealthController.class,
        AuthController.class,
        FailureEndpoint.class
    })
    static class TestApplication {}

    /** Generates a genuine servlet error dispatch without involving authentication. */
    @RestController
    static class FailureEndpoint {
        @GetMapping("/rate-limit-test/fail")
        String fail() {
            throw new IllegalStateException("Expected error-dispatch test failure");
        }
    }
}
