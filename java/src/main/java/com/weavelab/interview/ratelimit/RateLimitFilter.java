package com.weavelab.interview.ratelimit;

import static com.weavelab.interview.ratelimit.RateLimitResponseWriter.unavailable;
import static com.weavelab.interview.ratelimit.RateLimitResponseWriter.writeDecision;

import com.weavelab.interview.config.RateLimitProperties;
import com.weavelab.interview.ratelimit.strategy.model.RateLimitDecision;
import com.weavelab.interview.ratelimit.strategy.model.RateLimitDecisionEvent;

import jakarta.servlet.DispatcherType;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.UUID;
import java.util.function.Consumer;

/** One global decision per original request; queued grants resume on container threads. */
public abstract class RateLimitFilter extends OncePerRequestFilter {
    private static final Logger LOG = LoggerFactory.getLogger(RateLimitFilter.class);
    static final String GRANTED = RateLimitFilter.class.getName() + ".granted";
    private final RateLimitProperties properties;
    private final Clock clock;

    protected RateLimitFilter(RateLimitProperties properties, Clock clock) {
        this.properties = properties;
        this.clock = clock;
    }

    @Override
    protected boolean shouldNotFilterAsyncDispatch() {
        return false;
    }

    @Override
    protected final void doFilterInternal(
            HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        if (request.getDispatcherType() == DispatcherType.ERROR) {
            chain.doFilter(request, response);
        } else if (request.getDispatcherType() == DispatcherType.ASYNC) {
            if (Boolean.TRUE.equals(request.getAttribute(GRANTED))) {
                chain.doFilter(request, response);
            } else {
                unavailable(response, "No rate limit grant");
            }
        } else {
            handleRequest(request, response, chain, observerFor(request));
        }
    }

    protected final void handleDecision(
            RateLimitDecision decision,
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws IOException, ServletException {
        if (decision.outcome() == RateLimitDecision.Outcome.ALLOWED) {
            request.setAttribute(GRANTED, true);
            chain.doFilter(request, response);
        } else {
            writeDecision(response, decision);
        }
    }

    protected abstract void handleRequest(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain,
            Consumer<RateLimitDecisionEvent> observer)
            throws IOException, ServletException;

    private Consumer<RateLimitDecisionEvent> observerFor(HttpServletRequest request) {
        Instant requestTime = clock.instant();
        String requestId = UUID.randomUUID().toString();
        String method = request.getMethod();
        return event ->
                LOG.info(
                        "event=rate_limit_decision requestId={} requestTime={}"
                                + " decisionTime={} method={} approach={} capacity={}"
                                + " requests={} periodMs={} decision={} reason={}"
                                + " retryAfterMs={} waitedMs={} state={}",
                        requestId,
                        requestTime,
                        event.decisionTime(),
                        method,
                        properties.getApproach(),
                        properties.getCapacity(),
                        properties.getRequests(),
                        properties.getPeriod().toMillis(),
                        event.decision(),
                        event.reason(),
                        event.retryAfter().toMillis(),
                        event.waited().toMillis(),
                        event.state());
    }
}
