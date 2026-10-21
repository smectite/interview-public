package com.weavelab.interview.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Binds and validates rate-limit configuration. Capacity defaults to ten, the rate to ten per
 * minute, and queued waiting to six seconds.
 */
@ConfigurationProperties("app.rate-limit")
public class RateLimitProperties implements InitializingBean {
    private static final Logger LOG = LoggerFactory.getLogger(RateLimitProperties.class);

    /** Supported startup-selected admission algorithms. */
    public enum Approach {
        /** Elapsed-time debt bucket with immediate admission or rejection. */
        LAZY_DRAINING,
        /** Exact accepted-request timestamp window. */
        SLIDING_WINDOW,
        /** Token bucket with FIFO waiting and bounded deadlines. */
        BOUNDED_QUEUE
    }

    private Approach approach = Approach.LAZY_DRAINING;
    private int capacity = 10;
    private int requests = 10;
    private Duration period = Duration.ofMinutes(1);
    private Duration queueTimeout = Duration.ofSeconds(6);

    /** Validates capacity, strategy and duration bounds before the application starts. */
    @Override
    public void afterPropertiesSet() {
        LOG.info("event=method_invocation method=RateLimitProperties.afterPropertiesSet");
        if (approach == null || (capacity != 1 && capacity != 10) || requests <= 0) {
            throw new IllegalArgumentException(
                    "Rate limit requires an approach, capacity 1 or 10 and positive requests");
        }
        try {
            long nanos = period.toNanos();
            if (nanos <= 0 || nanos % requests != 0 || queueTimeout.toNanos() <= 0) {
                throw new IllegalArgumentException(
                        "Rate limit durations must be positive and period divisible by requests");
            }
            Math.multiplyExact(nanos / requests, capacity);
        } catch (ArithmeticException | NullPointerException e) {
            throw new IllegalArgumentException("Rate limit durations exceed supported bounds", e);
        }
    }

    /** Returns the per-request drainage or token-refill interval. */
    public Duration interval() {
        LOG.info("event=method_invocation method=RateLimitProperties.interval");
        return period.dividedBy(requests);
    }

    /** Returns the selected admission strategy. */
    public Approach getApproach() {
        LOG.info("event=method_invocation method=RateLimitProperties.getApproach");
        return approach;
    }

    /** Sets the startup-selected admission strategy. */
    public void setApproach(Approach approach) {
        LOG.info("event=method_invocation method=RateLimitProperties.setApproach");
        this.approach = approach;
    }

    /** Returns the burst/token capacity and queue waiting bound. */
    public int getCapacity() {
        LOG.info("event=method_invocation method=RateLimitProperties.getCapacity");
        return capacity;
    }

    /** Sets the validated burst/token capacity and queue waiting bound. */
    public void setCapacity(int capacity) {
        LOG.info("event=method_invocation method=RateLimitProperties.setCapacity");
        this.capacity = capacity;
    }

    /** Returns the allowed request count per configured period. */
    public int getRequests() {
        LOG.info("event=method_invocation method=RateLimitProperties.getRequests");
        return requests;
    }

    /** Sets the allowed request count per configured period. */
    public void setRequests(int requests) {
        LOG.info("event=method_invocation method=RateLimitProperties.setRequests");
        this.requests = requests;
    }

    /** Returns the rate period or sliding-window duration. */
    public Duration getPeriod() {
        LOG.info("event=method_invocation method=RateLimitProperties.getPeriod");
        return period;
    }

    /** Sets the rate period or sliding-window duration. */
    public void setPeriod(Duration period) {
        LOG.info("event=method_invocation method=RateLimitProperties.setPeriod");
        this.period = period;
    }

    /** Returns the maximum enqueue-relative waiting duration. */
    public Duration getQueueTimeout() {
        LOG.info("event=method_invocation method=RateLimitProperties.getQueueTimeout");
        return queueTimeout;
    }

    /** Sets the maximum enqueue-relative waiting duration. */
    public void setQueueTimeout(Duration queueTimeout) {
        LOG.info("event=method_invocation method=RateLimitProperties.setQueueTimeout");
        this.queueTimeout = queueTimeout;
    }
}
