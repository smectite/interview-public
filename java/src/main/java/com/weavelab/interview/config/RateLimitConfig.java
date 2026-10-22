package com.weavelab.interview.config;

import com.weavelab.interview.ratelimit.service.IRateLimiter;
import com.weavelab.interview.ratelimit.service.impl.BoundedQueueRateLimiter;
import com.weavelab.interview.ratelimit.service.impl.LazyDrainingRateLimiter;
import com.weavelab.interview.ratelimit.service.impl.SlidingWindowRateLimiter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.function.LongSupplier;

/**
 * Creates the process-local limiter singleton and its externally managed queue scheduler. The
 * configured strategy is selected once at startup; Spring closes the limiter before its executor.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(RateLimitProperties.class)
public class RateLimitConfig {
    private static final Logger LOG = LoggerFactory.getLogger(RateLimitConfig.class);

    @Bean(destroyMethod = "shutdownNow")
    ScheduledThreadPoolExecutor rateLimitScheduler() {
        ScheduledThreadPoolExecutor scheduler =
                new ScheduledThreadPoolExecutor(
                        1,
                        task -> {
                            Thread thread = new Thread(task, "rate-limit-scheduler");
                            thread.setDaemon(true);
                            return thread;
                        });
        scheduler.setRemoveOnCancelPolicy(true);
        scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        return scheduler;
    }

    @Bean(destroyMethod = "close")
    /** Selects the shared limiter implementation once for this application process. */
    public IRateLimiter rateLimiter(
            RateLimitProperties properties, ScheduledThreadPoolExecutor rateLimitScheduler) {
        LOG.info("event=method_invocation method=RateLimitConfig.rateLimiter");
        LongSupplier ticker = System::nanoTime;
        return switch (properties.getApproach()) {
            case LAZY_DRAINING ->
                    new LazyDrainingRateLimiter(
                            properties.getCapacity(), properties.interval(), ticker);
            case SLIDING_WINDOW ->
                    new SlidingWindowRateLimiter(
                            properties.getRequests(), properties.getPeriod(), ticker);
            case BOUNDED_QUEUE ->
                    new BoundedQueueRateLimiter(
                            properties.getCapacity(),
                            properties.interval(),
                            properties.getQueueTimeout(),
                            ticker,
                            rateLimitScheduler);
        };
    }
}
