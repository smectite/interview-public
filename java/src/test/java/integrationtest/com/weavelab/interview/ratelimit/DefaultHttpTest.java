package integrationtest.com.weavelab.interview.ratelimit;

import static org.assertj.core.api.Assertions.assertThat;

import com.weavelab.interview.auth.AuthFilter;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.ActiveProfiles;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

/** Verifies all clients and endpoints share the default immediate quota. */
@ActiveProfiles("rate-limit-http-test")
@SpringBootTest(
        classes = QueueHttpTest.TestApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"app.rate-limit.approach=LAZY_DRAINING", "app.rate-limit.capacity=10"})
class DefaultHttpTest {
    @LocalServerPort private int port;

    @SpyBean private AuthFilter authFilter;

    /**
     * Ten health admissions exhaust one shared quota; another identity and POST auth route receive
     * 429, Retry-After and no-store. The default HTTP suite also verifies that a rejected request
     * never reaches the authentication filter.
     */
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
            Mockito.clearInvocations(authFilter);
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
            Mockito.verifyNoInteractions(authFilter);
            assertThat(second.headers().firstValue("Retry-After"))
                    .hasValueSatisfying(
                            value -> assertThat(Integer.parseInt(value)).isBetween(1, 6));
            assertThat(second.headers().firstValue("Cache-Control")).hasValue("no-store");
        }
    }
}
