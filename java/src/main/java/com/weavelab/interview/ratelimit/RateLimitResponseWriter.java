package com.weavelab.interview.ratelimit;

import com.weavelab.interview.ratelimit.strategy.model.RateLimitDecision;

import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.time.Duration;

/** Shared HTTP response policy for immediate and queued decisions. */
final class RateLimitResponseWriter {
    private RateLimitResponseWriter() {}

    static void writeDecision(HttpServletResponse response, RateLimitDecision decision)
            throws IOException {
        if (decision.outcome() == RateLimitDecision.Outcome.RATE_LIMITED) {
            response.setHeader("Retry-After", Long.toString(ceilSeconds(decision.retryAfter())));
            write(response, 429, "Too many requests");
        } else {
            unavailable(
                    response,
                    decision.outcome() == RateLimitDecision.Outcome.TIMED_OUT
                            ? "Rate limit queue timeout"
                            : "Rate limiter unavailable");
        }
    }

    static void unavailable(HttpServletResponse response, String error) throws IOException {
        write(response, 503, error);
    }

    private static void write(HttpServletResponse response, int status, String error)
            throws IOException {
        if (response.isCommitted()) return;
        response.setStatus(status);
        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");
        response.setHeader("Cache-Control", "no-store");
        response.getWriter().write("{\"error\":\"" + error + "\"}");
    }

    private static long ceilSeconds(Duration duration) {
        return Math.max(1, duration.getSeconds() + (duration.getNano() > 0 ? 1 : 0));
    }
}
