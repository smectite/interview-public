package com.weavelab.interview.ratelimit;

import static com.weavelab.interview.ratelimit.RateLimitResponseWriter.unavailable;
import static com.weavelab.interview.ratelimit.RateLimitResponseWriter.writeDecision;

import com.weavelab.interview.config.RateLimitProperties;
import com.weavelab.interview.ratelimit.strategy.QueuedRateLimiter;
import com.weavelab.interview.ratelimit.strategy.Submission;
import com.weavelab.interview.ratelimit.strategy.model.RateLimitDecision;
import com.weavelab.interview.ratelimit.strategy.model.RateLimitDecisionEvent;

import jakarta.servlet.AsyncContext;
import jakarta.servlet.AsyncEvent;
import jakarta.servlet.AsyncListener;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/** HTTP adapter for bounded queue submissions and async continuation. */
public final class QueuedRateLimitFilter extends RateLimitFilter {
    private static final Logger LOG = LoggerFactory.getLogger(QueuedRateLimitFilter.class);

    private final QueuedRateLimiter limiter;
    private final Duration timeout;

    /** Creates the configured adapter or value using the supplied dependencies. */
    public QueuedRateLimitFilter(
            QueuedRateLimiter limiter, RateLimitProperties properties, Clock clock) {
        super(properties, clock);
        LOG.info("event=method_invocation method=QueuedRateLimitFilter.<init>");
        this.limiter = limiter;
        this.timeout = properties.getQueueTimeout();
    }

    @Override
    protected void handleRequest(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain,
            Consumer<RateLimitDecisionEvent> observer)
            throws IOException, ServletException {
        var submission = limiter.submit(observer);
        var future = submission.decision().toCompletableFuture();

        if (!future.isDone()) {
            handlePending(submission, request, response);
        } else {
            RateLimitDecision decision;
            try {
                decision = future.join(); // Only queued admission exposes asynchronous completion.
            } catch (RuntimeException failure) {
                LOG.error("Queued rate limit decision failed", failure);
                submission.cancel();
                unavailable(response, "Rate limiter unavailable");
                return;
            }
            handleDecision(decision, request, response, chain);
        }
    }

    private void handlePending(
            Submission submission, HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        if (!request.isAsyncSupported()) {
            submission.cancel();
            unavailable(response, "Asynchronous processing unavailable");
        } else {
            AsyncContext async;
            try {
                async = request.startAsync();
            } catch (IllegalStateException e) {
                LOG.warn("Unable to start asynchronous rate limit processing", e);
                submission.cancel();
                unavailable(response, "Asynchronous processing unavailable");
                return;
            }
            AtomicBoolean finished = new AtomicBoolean();
            async.setTimeout(ceilMillis(timeout));
            async.addListener(
                    new AsyncListener() {
                        /**
                         * Settles an overdue servlet wait once and completes its error response.
                         */
                        @Override
                        public void onTimeout(AsyncEvent event) throws IOException {
                            LOG.info(
                                    "event=method_invocation"
                                        + " method=QueuedRateLimitFilter.AsyncListener.onTimeout");
                            if (finished.compareAndSet(false, true)) {
                                LOG.warn("Asynchronous rate limit wait timed out");
                                submission.timeout();
                                try {
                                    unavailable(response, "Rate limit queue timeout");
                                } finally {
                                    complete(async);
                                }
                            }
                        }

                        /** Cancels pending admission after a servlet failure or disconnect. */
                        @Override
                        public void onError(AsyncEvent event) {
                            LOG.info(
                                    "event=method_invocation"
                                        + " method=QueuedRateLimitFilter.AsyncListener.onError");
                            if (finished.compareAndSet(false, true)) {
                                LOG.warn(
                                        "Asynchronous rate limit request failed or disconnected",
                                        event.getThrowable());
                                submission.cancel();
                                complete(async);
                            }
                        }

                        /**
                         * Cancels any admission still pending when the servlet request completes.
                         */
                        @Override
                        public void onComplete(AsyncEvent event) {
                            LOG.info(
                                    "event=method_invocation"
                                        + " method=QueuedRateLimitFilter.AsyncListener.onComplete");
                            if (finished.compareAndSet(false, true)) submission.cancel();
                        }

                        /** Observes a new async cycle; no additional admission is charged. */
                        @Override
                        public void onStartAsync(AsyncEvent event) {
                            LOG.info(
                                    "event=method_invocation"
                                        + " method=QueuedRateLimitFilter.AsyncListener.onStartAsync");
                        }
                    });
            // Listener and AsyncContext exist before observing a possibly concurrent release.
            submission
                    .decision()
                    .whenComplete(
                            (decision, failure) -> {
                                if (!finished.compareAndSet(false, true)) return;
                                try {
                                    if (failure != null) {
                                        LOG.error("Pending rate limit decision failed", failure);
                                        unavailable(response, "Rate limiter unavailable");
                                        complete(async);
                                    } else if (decision.outcome()
                                            == RateLimitDecision.Outcome.ALLOWED) {
                                        request.setAttribute(RateLimitFilter.GRANTED, true);
                                        async.dispatch();
                                    } else {
                                        writeDecision(response, decision);
                                        complete(async);
                                    }
                                } catch (IOException | IllegalStateException e) {
                                    LOG.warn(
                                            "Unable to write or dispatch queued rate limit"
                                                + " response",
                                            e);
                                    complete(async); // Container may already have closed a
                                    // disconnected
                                    // request.
                                }
                            });
        }
    }

    private long ceilMillis(Duration duration) {
        long millis = duration.toMillis();
        return Math.max(1, millis + (duration.getNano() % 1_000_000 > 0 ? 1 : 0));
    }

    private void complete(AsyncContext async) {
        try {
            async.complete();
        } catch (IllegalStateException failure) {
            LOG.debug("Async rate limit context was already completed or closed", failure);
        }
    }
}
