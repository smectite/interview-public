package unittest.com.weavelab.interview.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.weavelab.interview.auth.AuthFilter;
import com.weavelab.interview.config.FilterConfig;
import com.weavelab.interview.config.RateLimitConfig;
import com.weavelab.interview.config.RateLimitProperties;
import com.weavelab.interview.ratelimit.ImmediateRateLimitFilter;
import com.weavelab.interview.ratelimit.QueuedRateLimitFilter;
import com.weavelab.interview.ratelimit.service.IImmediateRateLimiter;
import com.weavelab.interview.ratelimit.service.IQueuedRateLimiter;
import com.weavelab.interview.ratelimit.service.IRateLimiter;
import com.weavelab.interview.ratelimit.service.ISubmission;
import com.weavelab.interview.ratelimit.service.model.RateLimitDecision.Outcome;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.web.servlet.FilterRegistrationBean;

import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicReference;

/** Verifies configuration defaults, adapter selection and startup validation. */
class RateLimitConfigTest {
    private final ApplicationContextRunner runner =
            new ApplicationContextRunner()
                    .withUserConfiguration(
                            RateLimitConfig.class, FilterConfig.class, AuthFilter.class);

    /**
     * Default lazy strategy, capacity ten, six-second queue timeout and singleton-shared
     * exhaustion.
     */
    @Test
    void defaultsAllowTenImmediateRequestsAndUseSixSecondQueueTimeout() {
        runner.run(
                context -> {
                    assertThat(context).hasNotFailed();
                    var properties = context.getBean(RateLimitProperties.class);
                    assertThat(properties.getCapacity()).isEqualTo(10);
                    assertThat(properties.getQueueTimeout()).isEqualTo(Duration.ofSeconds(6));
                    var limiter = context.getBean(IRateLimiter.class);
                    assertThat(limiter.getClass().getSimpleName())
                            .isEqualTo("LazyDrainingRateLimiter");
                    for (int i = 0; i < 10; i++)
                        assertThat(outcome(limiter)).isEqualTo(Outcome.ALLOWED);
                    assertThat(outcome(context.getBean(IRateLimiter.class)))
                            .isEqualTo(Outcome.RATE_LIMITED);
                });
    }

    /**
     * All three strategy names select the correct implementation and immediate/queued filter;
     * exactly two filter registrations exist; default token and waiting capacities are forwarded.
     */
    @ParameterizedTest
    @ValueSource(strings = {"LAZY_DRAINING", "SLIDING_WINDOW", "BOUNDED_QUEUE"})
    void selectsOneMatchingStrategyAndFilter(String approach) {
        runner.withPropertyValues("app.rate-limit.approach=" + approach)
                .run(
                        context -> {
                            assertThat(context).hasNotFailed();
                            var limiter = context.getBean(IRateLimiter.class);
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
                            if (limiter instanceof IQueuedRateLimiter queued) {
                                for (int i = 0; i < 9; i++)
                                    assertThat(outcome(limiter)).isEqualTo(Outcome.ALLOWED);
                                for (int i = 0; i < 10; i++)
                                    assertThat(queued.submit().decision().toCompletableFuture())
                                            .isNotDone();
                            } else {
                                assertThat(limiter).isInstanceOf(IImmediateRateLimiter.class);
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

    /**
     * Nine invalid overrides fail startup: unknown and removed strategies, unsupported capacity,
     * zero requests, zero/negative period, zero queue timeout, non-divisible interval and
     * unrepresentable duration.
     */
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

    private static Outcome outcome(IRateLimiter limiter) {
        if (limiter instanceof IImmediateRateLimiter immediate)
            return immediate.tryAcquire().outcome();
        if (limiter instanceof IQueuedRateLimiter queued)
            return queued.submit().decision().toCompletableFuture().join().outcome();
        throw new AssertionError("Unknown admission contract");
    }

    /** Nondefault capacity, request count, period and timeout reach the selected implementation. */
    @ParameterizedTest
    @ValueSource(strings = {"LAZY_DRAINING", "SLIDING_WINDOW", "BOUNDED_QUEUE"})
    void customSettingsReachEachStrategy(String approach) {
        runner.withPropertyValues(
                        "app.rate-limit.approach=" + approach,
                        "app.rate-limit.capacity=1",
                        "app.rate-limit.requests=2",
                        "app.rate-limit.period=200s",
                        "app.rate-limit.queue-timeout=75s")
                .run(
                        context -> {
                            assertThat(context).hasNotFailed();
                            var limiter = context.getBean(IRateLimiter.class);
                            var events =
                                    new ArrayList<
                                            com.weavelab.interview.ratelimit.service.model
                                                    .RateLimitDecisionEvent>();
                            if (limiter instanceof IImmediateRateLimiter immediate) {
                                immediate.tryAcquire(events::add);
                                if (approach.equals("LAZY_DRAINING")) {
                                    assertThat(events.get(0).state().get("maxDebtNs"))
                                            .isEqualTo(100_000_000_000L);
                                    assertThat(immediate.tryAcquire().outcome())
                                            .isEqualTo(Outcome.RATE_LIMITED);
                                } else {
                                    assertThat(events.get(0).state().get("windowLimit"))
                                            .isEqualTo(2L);
                                    assertThat(events.get(0).state().get("windowPeriodNs"))
                                            .isEqualTo(200_000_000_000L);
                                    assertThat(immediate.tryAcquire().outcome())
                                            .isEqualTo(Outcome.ALLOWED);
                                    assertThat(immediate.tryAcquire().outcome())
                                            .isEqualTo(Outcome.RATE_LIMITED);
                                }
                            } else {
                                var queued = (IQueuedRateLimiter) limiter;
                                queued.submit();
                                var waiting = queued.submit(events::add);
                                assertThat(waiting.decision().toCompletableFuture()).isNotDone();
                                assertThat(events.get(0).state().get("tokenCapacity"))
                                        .isEqualTo(1L);
                                assertThat(events.get(0).state().get("nextTokenInNs"))
                                        .isBetween(90_000_000_000L, 100_000_000_000L);
                                assertThat(events.get(0).state().get("deadlineRemainingNs"))
                                        .isEqualTo(75_000_000_000L);
                            }
                        });
    }

    /**
     * Valid individual durations still fail startup if capacity multiplied by interval overflows.
     */
    @Test
    void capacityDebtOverflowFailsStartup() {
        runner.withPropertyValues(
                        "app.rate-limit.capacity=10",
                        "app.rate-limit.requests=1",
                        "app.rate-limit.period=1000000000s")
                .run(context -> assertThat(context).hasFailed());
    }

    /**
     * Closing Spring settles pending requests, stops admissions and shuts down its owned scheduler.
     */
    @Test
    void contextShutdownStopsQueueAndScheduler() {
        var limiter = new AtomicReference<IQueuedRateLimiter>();
        var pending = new AtomicReference<ISubmission>();
        var scheduler = new AtomicReference<ScheduledThreadPoolExecutor>();
        runner.withPropertyValues(
                        "app.rate-limit.approach=BOUNDED_QUEUE",
                        "app.rate-limit.capacity=1",
                        "app.rate-limit.requests=1",
                        "app.rate-limit.period=200s",
                        "app.rate-limit.queue-timeout=100s")
                .run(
                        context -> {
                            limiter.set((IQueuedRateLimiter) context.getBean(IRateLimiter.class));
                            scheduler.set(context.getBean(ScheduledThreadPoolExecutor.class));
                            limiter.get().submit();
                            pending.set(limiter.get().submit());
                            assertThat(pending.get().decision().toCompletableFuture()).isNotDone();
                            assertThat(scheduler.get().getQueue()).hasSize(1);
                        });
        assertThat(pending.get().decision().toCompletableFuture().join().outcome())
                .isEqualTo(Outcome.STOPPED);
        assertThat(limiter.get().submit().decision().toCompletableFuture().join().outcome())
                .isEqualTo(Outcome.STOPPED);
        assertThat(scheduler.get().isShutdown()).isTrue();
        assertThat(scheduler.get().getQueue()).isEmpty();
    }
}
