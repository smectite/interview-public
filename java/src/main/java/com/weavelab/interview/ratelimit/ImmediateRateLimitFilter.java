package com.weavelab.interview.ratelimit;

import com.weavelab.interview.config.RateLimitProperties;
import com.weavelab.interview.ratelimit.service.IImmediateRateLimiter;
import com.weavelab.interview.ratelimit.service.model.RateLimitDecisionEvent;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Clock;
import java.util.function.Consumer;

/** Immediate admission or rejection without servlet async processing. */
public final class ImmediateRateLimitFilter extends RateLimitFilter {
    private static final Logger LOG = LoggerFactory.getLogger(ImmediateRateLimitFilter.class);

    private final IImmediateRateLimiter limiter;

    /** Creates the configured adapter or value using the supplied dependencies. */
    public ImmediateRateLimitFilter(
            IImmediateRateLimiter limiter, RateLimitProperties properties, Clock clock) {
        super(properties, clock);
        LOG.info("event=method_invocation method=ImmediateRateLimitFilter.<init>");
        this.limiter = limiter;
    }

    @Override
    protected void handleRequest(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain,
            Consumer<RateLimitDecisionEvent> observer)
            throws IOException, ServletException {
        handleDecision(limiter.tryAcquire(observer), request, response, chain);
    }
}
