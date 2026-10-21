package com.weavelab.interview.ratelimit;

import static org.assertj.core.api.Assertions.assertThat;

import com.weavelab.interview.auth.AuthFilter;
import com.weavelab.interview.config.FilterConfig;
import com.weavelab.interview.config.RateLimitConfig;
import com.weavelab.interview.config.RateLimitProperties;
import com.weavelab.interview.ratelimit.strategy.ImmediateRateLimiter;
import com.weavelab.interview.ratelimit.strategy.QueuedRateLimiter;
import com.weavelab.interview.ratelimit.strategy.RateLimiter;
import com.weavelab.interview.ratelimit.strategy.model.RateLimitDecision.Outcome;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.web.servlet.FilterRegistrationBean;

import java.time.Duration;

/** Verifies configuration defaults, adapter selection and startup validation. */
class RateLimitConfigTest {
    private final ApplicationContextRunner runner =
            new ApplicationContextRunner()
                    .withUserConfiguration(
                            RateLimitConfig.class, FilterConfig.class, AuthFilter.class);

    @Test
    void defaultsAllowTenImmediateRequestsAndUseSixSecondQueueTimeout() {
        runner.run(
                context -> {
                    assertThat(context).hasNotFailed();
                    var properties = context.getBean(RateLimitProperties.class);
                    assertThat(properties.getCapacity()).isEqualTo(10);
                    assertThat(properties.getQueueTimeout()).isEqualTo(Duration.ofSeconds(6));
                    var limiter = context.getBean(RateLimiter.class);
                    assertThat(limiter.getClass().getSimpleName())
                            .isEqualTo("LazyDrainingRateLimiter");
                    for (int i = 0; i < 10; i++)
                        assertThat(outcome(limiter)).isEqualTo(Outcome.ALLOWED);
                    assertThat(outcome(context.getBean(RateLimiter.class)))
                            .isEqualTo(Outcome.RATE_LIMITED);
                });
    }

    @ParameterizedTest
    @ValueSource(strings = {"LAZY_DRAINING", "SLIDING_WINDOW", "BOUNDED_QUEUE"})
    void selectsOneMatchingStrategyAndFilter(String approach) {
        runner.withPropertyValues("app.rate-limit.approach=" + approach)
                .run(
                        context -> {
                            assertThat(context).hasNotFailed();
                            var limiter = context.getBean(RateLimiter.class);
                            var registration =
                                    context.getBean(
                                            "rateLimitFilterRegistration",
                                            FilterRegistrationBean.class);
                            assertThat(registration.getFilter())
                                    .isInstanceOf(
                                            approach.equals("BOUNDED_QUEUE")
                                                    ? QueuedRateLimitFilter.class
                                                    : ImmediateRateLimitFilter.class);
                            assertThat(context.getBeansOfType(FilterRegistrationBean.class))
                                    .hasSize(2);
                            assertThat(outcome(limiter)).isEqualTo(Outcome.ALLOWED);
                            if (limiter instanceof QueuedRateLimiter queued) {
                                for (int i = 0; i < 9; i++)
                                    assertThat(outcome(limiter)).isEqualTo(Outcome.ALLOWED);
                                for (int i = 0; i < 10; i++)
                                    assertThat(queued.submit().decision().toCompletableFuture())
                                            .isNotDone();
                            } else {
                                assertThat(limiter).isInstanceOf(ImmediateRateLimiter.class);
                                for (int i = 0; i < 10; i++)
                                    assertThat(outcome(limiter))
                                            .isEqualTo(
                                                    i < 9 ? Outcome.ALLOWED : Outcome.RATE_LIMITED);
                            }
                            assertThat(outcome(limiter)).isEqualTo(Outcome.RATE_LIMITED);
                            assertThat(limiter.getClass().getSimpleName())
                                    .isEqualTo(
                                            switch (approach) {
                                                case "LAZY_DRAINING" -> "LazyDrainingRateLimiter";
                                                case "SLIDING_WINDOW" -> "SlidingWindowRateLimiter";
                                                default -> "BoundedQueueRateLimiter";
                                            });
                        });
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "approach=UNKNOWN",
                "approach=SCHEDULED_LEAKAGE",
                "capacity=2",
                "requests=0",
                "period=0s",
                "period=-1s",
                "queue-timeout=0s",
                "requests=7",
                "period=9999999999999999999s"
            })
    void invalidConfigurationFailsStartup(String setting) {
        runner.withPropertyValues("app.rate-limit." + setting)
                .run(context -> assertThat(context).hasFailed());
    }

    private static Outcome outcome(RateLimiter limiter) {
        if (limiter instanceof ImmediateRateLimiter immediate)
            return immediate.tryAcquire().outcome();
        if (limiter instanceof QueuedRateLimiter queued)
            return queued.submit().decision().toCompletableFuture().join().outcome();
        throw new AssertionError("Unknown admission contract");
    }
}
