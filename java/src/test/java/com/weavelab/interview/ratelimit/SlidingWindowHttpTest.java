package com.weavelab.interview.ratelimit;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.ActiveProfiles;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

/** Verifies the rolling-window quota through a real HTTP server. */
@ActiveProfiles("rate-limit-http-test")
@SpringBootTest(
        classes = QueueHttpTest.TestApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"app.rate-limit.approach=SLIDING_WINDOW", "app.rate-limit.capacity=10"})
class SlidingWindowHttpTest {
    @LocalServerPort private int port;

    @Test
    void differentClientsAndEndpointsShareDefaultQuotaBeforeAuthentication() throws Exception {
        {
            HttpClient client = HttpClient.newHttpClient();
            var first =
                    client.send(
                            HttpRequest.newBuilder(
                                            URI.create("http://127.0.0.1:" + port + "/health"))
                                    .header("Authorization", "Bearer alice@example.test")
                                    .build(),
                            HttpResponse.BodyHandlers.ofString());
            assertThat(first.statusCode()).isEqualTo(200);
            for (int i = 0; i < 9; i++) {
                var allowed =
                        client.send(
                                HttpRequest.newBuilder(
                                                URI.create("http://127.0.0.1:" + port + "/health"))
                                        .build(),
                                HttpResponse.BodyHandlers.ofString());
                assertThat(allowed.statusCode()).isEqualTo(200);
            }
            var second =
                    client.send(
                            HttpRequest.newBuilder(
                                            URI.create(
                                                    "http://127.0.0.1:" + port + "/api/auth/token"))
                                    .header("Authorization", "Bearer bob@example.test")
                                    .POST(HttpRequest.BodyPublishers.noBody())
                                    .build(),
                            HttpResponse.BodyHandlers.ofString());
            assertThat(first.statusCode()).isEqualTo(200);
            assertThat(second.statusCode()).isEqualTo(429);
            assertThat(second.headers().firstValue("Retry-After"))
                    .hasValueSatisfying(
                            value -> assertThat(Integer.parseInt(value)).isBetween(1, 60));
            assertThat(second.headers().firstValue("Cache-Control")).hasValue("no-store");
        }
    }
}
