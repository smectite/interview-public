package unittest.com.weavelab.interview.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.weavelab.interview.config.RateLimitProperties;
import com.weavelab.interview.ratelimit.service.IRateLimiter;
import com.weavelab.interview.ratelimit.service.impl.LazyDrainingRateLimiter;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.context.ConfigurationPropertiesAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.time.Duration;

/**
 * Verifies the configuration contract for the single lazy-draining limiter.
 *
 * <p>Each execution uses a fresh {@link ApplicationContextRunner} context containing only property
 * binding, {@link RateLimitProperties}, and {@link LazyDrainingRateLimiter}. This exercises actual
 * Spring binding and startup validation without starting a web server or database. Contexts and
 * limiter state are isolated between scenarios.
 *
 * <p>Coverage includes default settings, overrides of all three rate parameters, singleton
 * allowance sharing, arbitrary positive burst capacity, and eight invalid configurations. Timing
 * and concurrent admission behavior are covered separately by the limiter tests.
 */
class RateLimitPropertiesTest {
    /** Builds isolated contexts with configuration binding and the production limiter bean. */
    private final ApplicationContextRunner runner =
            new ApplicationContextRunner()
                    .withConfiguration(
                            AutoConfigurations.of(ConfigurationPropertiesAutoConfiguration.class))
                    .withUserConfiguration(
                            RateLimitProperties.class, LazyDrainingRateLimiter.class);

    /**
     * Starts with no rate-limit overrides and verifies capacity ten, ten requests, and a one-minute
     * period. Ten immediate attempts succeed; an eleventh attempt through a new bean lookup is
     * rejected, showing that callers share the singleton's allowance.
     */
    @Test
    void defaultsConfigureOneSharedTenRequestBucket() {
        runner.run(
                context -> {
                    assertThat(context).hasNotFailed();
                    var properties = context.getBean(RateLimitProperties.class);
                    assertThat(properties.getCapacity()).isEqualTo(10);
                    assertThat(properties.getRequests()).isEqualTo(10);
                    assertThat(properties.getPeriod()).isEqualTo(Duration.ofMinutes(1));
                    var limiter = context.getBean(IRateLimiter.class);
                    assertThat(limiter).isSameAs(context.getBean(LazyDrainingRateLimiter.class));
                    for (int i = 0; i < 10; i++)
                        assertThat(limiter.tryAcquire().allowed()).isTrue();
                    assertThat(
                                    context.getBean(LazyDrainingRateLimiter.class)
                                            .tryAcquire()
                                            .allowed())
                            .isFalse();
                });
    }

    /**
     * Overrides capacity, request count and period together using Spring property strings. Capacity
     * three and four requests per two seconds produce a configured interval of 500 milliseconds.
     * The constructed limiter admits three immediate attempts and rejects the fourth, demonstrating
     * that capacity is not restricted to the default value.
     */
    @Test
    void allRateParametersAreBoundAndArbitraryCapacityIsSupported() {
        runner.withPropertyValues(
                        "app.rate-limit.capacity=3",
                        "app.rate-limit.requests=4",
                        "app.rate-limit.period=2s")
                .run(
                        context -> {
                            assertThat(context).hasNotFailed();
                            var properties = context.getBean(RateLimitProperties.class);
                            assertThat(properties.getCapacity()).isEqualTo(3);
                            assertThat(properties.interval()).isEqualTo(Duration.ofMillis(500));
                            var limiter = context.getBean(IRateLimiter.class);
                            assertThat(limiter)
                                    .isSameAs(context.getBean(LazyDrainingRateLimiter.class));
                            for (int i = 0; i < 3; i++)
                                assertThat(limiter.tryAcquire().allowed()).isTrue();
                            assertThat(limiter.tryAcquire().allowed()).isFalse();
                        });
    }

    /**
     * Verifies that each invalid override prevents the application context from starting.
     * Unspecified properties retain their defaults, isolating the invalid setting per case.
     *
     * <ul>
     *   <li>{@code capacity=0} and {@code capacity=-1}: burst size must be positive.
     *   <li>{@code requests=0}: the request count must be positive.
     *   <li>{@code period=0s} and {@code period=-1s}: the period must be positive.
     *   <li>{@code requests=7}: the default minute cannot divide exactly into seven
     *       integer-nanosecond intervals.
     *   <li>{@code period=9999999999999999999s}: the duration exceeds supported bounds.
     *   <li>{@code capacity=2147483647}: capacity times the default interval overflows the maximum
     *       representable drain-time debt.
     * </ul>
     *
     * @param setting invalid rate-limit property assignment for this execution
     */
    @ParameterizedTest
    @ValueSource(
            strings = {
                "capacity=0",
                "capacity=-1",
                "requests=0",
                "period=0s",
                "period=-1s",
                "requests=7",
                "period=9999999999999999999s",
                "capacity=2147483647"
            })
    void invalidSettingsFailStartup(String setting) {
        runner.withPropertyValues("app.rate-limit." + setting)
                .run(context -> assertThat(context).hasFailed());
    }
}
