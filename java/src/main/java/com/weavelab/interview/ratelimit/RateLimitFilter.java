package com.weavelab.interview.ratelimit;

import com.weavelab.interview.ratelimit.service.IRateLimiter;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.UUID;

/**
 * Applies the one global bucket before authentication ({@code AuthFilter} has order one). Initial
 * requests are charged once; async and error dispatches are skipped by the inherited
 * OncePerRequestFilter defaults. Admission is synchronous and never waits for future capacity.
 */
@Component
@Order(0)
public final class RateLimitFilter extends OncePerRequestFilter {
    private static final Logger LOG = LoggerFactory.getLogger(RateLimitFilter.class);
    private final IRateLimiter limiter;
    private final Clock clock;

    /**
     * @param limiter process-wide bucket shared by every client and route
     */
    @Autowired
    public RateLimitFilter(IRateLimiter limiter) {
        this(limiter, Clock.systemUTC());
    }

    /**
     * Creates the HTTP adapter with an injectable diagnostic clock.
     *
     * @param limiter process-wide bucket
     * @param clock wall-clock source for request timestamps only
     */
    public RateLimitFilter(IRateLimiter limiter, Clock clock) {
        LOG.info("event=method_invocation method=RateLimitFilter.<init>");
        this.limiter = limiter;
        this.clock = clock;
    }

    /** Records an atomic decision and either continues the chain or returns 429 immediately. */
    @Override
    protected void doFilterInternal(
            HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        Instant requestTime = clock.instant();
        var decision = limiter.tryAcquire();
        LOG.info(
                "event=rate_limit_decision requestId={} requestTime={} decisionTime={} method={}"
                        + " decision={} retryAfterMs={} debtBeforeDrainNs={} debtAfterDrainNs={}"
                        + " debtAfterDecisionNs={} maxDebtNs={} elapsedNs={}",
                UUID.randomUUID(),
                requestTime,
                decision.decisionTime(),
                request.getMethod(),
                decision.allowed() ? "ALLOWED" : "RATE_LIMITED",
                decision.retryAfter().toMillis(),
                decision.debtBeforeDrainNs(),
                decision.debtAfterDrainNs(),
                decision.debtAfterDecisionNs(),
                decision.maxDebtNs(),
                decision.elapsedNs());
        if (decision.allowed()) {
            chain.doFilter(request, response);
        } else {
            long retrySeconds =
                    decision.retryAfter().getSeconds()
                            + (decision.retryAfter().getNano() > 0 ? 1 : 0);
            response.setStatus(429);
            response.setHeader("Retry-After", Long.toString(Math.max(1, retrySeconds)));
            response.setHeader("Cache-Control", "no-store");
            response.setContentType("application/json");
            response.setCharacterEncoding("UTF-8");
            response.getWriter().write("{\"error\":\"Too many requests\"}");
        }
    }
}
