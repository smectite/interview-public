package com.weavelab.interview.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.Duration;

/** Binds the single lazy-draining limiter's burst size and sustained request rate. */
@Component
@ConfigurationProperties("app.rate-limit")
public class RateLimitProperties implements InitializingBean {
    private static final Logger LOG = LoggerFactory.getLogger(RateLimitProperties.class);
    private int capacity = 10;
    private int requests = 10;
    private Duration period = Duration.ofMinutes(1);

    /** Validates positive settings, exact interval division and representable bucket debt. */
    @Override
    public void afterPropertiesSet() {
        LOG.info("event=method_invocation method=RateLimitProperties.afterPropertiesSet");
        try {
            if (capacity <= 0
                    || requests <= 0
                    || period == null
                    || period.toNanos() <= 0
                    || period.toNanos() % requests != 0) {
                throw new IllegalArgumentException(
                        "Capacity, requests and period must be positive; period must divide exactly"
                            + " by requests in nanoseconds");
            }
            Math.multiplyExact(period.toNanos() / requests, capacity);
        } catch (RuntimeException failure) {
            LOG.error("Invalid lazy-draining rate limit configuration", failure);
            throw new IllegalArgumentException(
                    "Invalid lazy-draining rate limit configuration", failure);
        }
    }

    /**
     * @return drainage time per accepted request
     */
    public Duration interval() {
        LOG.info("event=method_invocation method=RateLimitProperties.interval");
        return period.dividedBy(requests);
    }

    /**
     * @return maximum burst size
     */
    public int getCapacity() {
        LOG.info("event=method_invocation method=RateLimitProperties.getCapacity");
        return capacity;
    }

    /**
     * @param capacity positive burst size, validated after binding
     */
    public void setCapacity(int capacity) {
        LOG.info("event=method_invocation method=RateLimitProperties.setCapacity");
        this.capacity = capacity;
    }

    /**
     * @return sustained request count per period
     */
    public int getRequests() {
        LOG.info("event=method_invocation method=RateLimitProperties.getRequests");
        return requests;
    }

    /**
     * @param requests positive request count, validated after binding
     */
    public void setRequests(int requests) {
        LOG.info("event=method_invocation method=RateLimitProperties.setRequests");
        this.requests = requests;
    }

    /**
     * @return configured rate period
     */
    public Duration getPeriod() {
        LOG.info("event=method_invocation method=RateLimitProperties.getPeriod");
        return period;
    }

    /**
     * @param period positive rate period, validated after binding
     */
    public void setPeriod(Duration period) {
        LOG.info("event=method_invocation method=RateLimitProperties.setPeriod");
        this.period = period;
    }
}
