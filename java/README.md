# CRM API: process-local request rate limiting

Java 17 and Spring Boot 3.3. Run `./mvnw test` (or `make test`). No additional dependencies are required.

## Configuration

Defaults in `src/main/resources/application.properties`:

```properties
app.rate-limit.approach=LAZY_DRAINING
app.rate-limit.capacity=10
app.rate-limit.requests=10
app.rate-limit.period=60s
app.rate-limit.queue-timeout=6s
```

Select `LAZY_DRAINING`, `SLIDING_WINDOW`, or `BOUNDED_QUEUE`. `SCHEDULED_LEAKAGE` has been removed and its configuration value fails startup. The `RateLimitConfig.rateLimiter` factory selects one process-local singleton at startup. `FilterConfig` selects and registers exactly one matching rate-limit filter, before authentication. Restart to change configuration; restart resets rate state. Multiple application processes do not share state.

All clients, HTTP methods and routes share the limiter, including `/health`, `/api/auth/token`, unauthenticated attempts and uploads. An unauthorized attempt consumes an admission; an exhausted quota returns 429 before authentication. Downstream failures do not refund admissions. This policy does not limit already executing requests.

| Strategy | Admission behavior | HTTP workflow |
| --- | --- | --- |
| `LAZY_DRAINING` (default) | Capacity ten allows an initial burst of ten, then recovers one admission every six seconds | Synchronous admission or rejection |
| `SLIDING_WINDOW` | At most ten accepted requests in any rolling minute | Synchronous admission or rejection |
| `BOUNDED_QUEUE` | Ten initial tokens; consume immediately or wait FIFO for a token, replenished every six seconds | Immediate result or servlet async continuation |

**Capacity ten lazy draining does not guarantee ten requests per rolling minute.** Select `SLIDING_WINDOW` for that guarantee. For example, lazy draining can admit ten requests at time zero and another at six seconds, whereas sliding window rejects the eleventh until the oldest admission is sixty seconds old.

`capacity` must be 1 or 10 and defaults to 10. It controls lazy-bucket burst capacity. In queue mode it independently bounds stored tokens and waiting entries: capacity ten means up to ten tokens plus up to ten waiting requests. Sliding window uses `requests` and `period` as its quota; `capacity` does not change that quota. The nominal release interval is `period / requests`, six seconds by default. Durations and request counts must be positive; the period must divide exactly into integer-nanosecond intervals. Invalid settings fail startup.

## Algorithms and timing

### Lazy draining

Subtract elapsed monotonic time from drain-time debt on each request, clamp at zero and admit only if another request fits. No background tasks run. Capacity one remains available when strict six-second spacing is preferred over bursts.

### Sliding window

Keep a deque of accepted request timestamps. Under one short lock, remove timestamps whose age is at least the configured period, reject if the deque still contains `requests` entries, or append the current timestamp. Rejections do not extend the window. `Retry-After` describes when the oldest retained admission expires.

The rolling interval is `(now - period, now]`: an admission exactly sixty seconds old no longer counts. The deque retains at most the configured number of accepted requests. Timestamp expiration is lazy; no scheduler runs. All calculations use an injected monotonic ticker rather than wall-clock time.

### Bounded queue

The bucket starts full with ten tokens. Each admission consumes one token. When tokens are exhausted, requests wait FIFO for replenishment or timeout; a full waiting queue rejects immediately. New arrivals cannot bypass older waiting requests, even if the scheduler is late.

Replenishment earns one whole token per six-second interval, up to capacity. Elapsed time is calculated lazily on submission or scheduler wake-up; unused partial intervals are retained while below capacity. Credit beyond a full bucket is discarded. Ten requests can run immediately, followed by queued releases as tokens return. Idle time restores up to ten tokens. A delayed callback may release several waiting requests using earned tokens; this strategy no longer promises six-second spacing between every release.

The default waiting timeout is **six seconds**, measured from enqueue. Expiration wins when a request's deadline coincides with token availability. For example, after all ten tokens are consumed at time zero, an eleventh request at time zero expires at six seconds. A request arriving at time one can take the token at time six after waiting five seconds. Expired or canceled requests do not consume tokens; already allowed requests do not refund tokens on cancellation. Queue capacity does not guarantee that every waiting request survives the timeout.

This token bucket allows bursts and does not guarantee a strict ten-request rolling-minute quota; select `SLIDING_WINDOW` for that requirement.

Pending requests use Servlet async processing instead of occupying request threads. Release resumes through container dispatch; authentication and controllers never run on the scheduler thread. Timeout, cancellation, disconnect, shutdown and scheduler rejection settle pending entries once. The scheduler considers the next token and earliest expiration, and runs only while requests wait. Client/proxy timeouts must accommodate the configured waiting period; disconnect detection depends on the container.

## HTTP adaptation and organization

- `RateLimitFilter`: abstract template for initial admission, redispatch guards, request metadata and shared immediate-response handling.
- `ImmediateRateLimitFilter`: selected for lazy draining and sliding window. Depends on `ImmediateRateLimiter.tryAcquire`, which returns a decision directly. It has no futures, submissions, cancellation or servlet async setup.
- `QueuedRateLimitFilter`: selected only for bounded queue. Handles immediate results and owns pending requests' async setup, timeout, cancellation, completion and dispatch races.
- `RateLimitResponseWriter`: shared response status, headers and JSON policy.
- `ratelimit.strategy`: limiter/submission contracts and immutable decisions/events.
- `ratelimit.strategy.impl`: the three implementations and package-private `LimiterSupport`.
- `RateLimitConfig`: algorithm factory and managed scheduler. Only queue mode schedules work.
- `FilterConfig`: startup filter factory, rate-limit registration and auth registration.
- `RateLimitProperties`: defaults and validation.

The admission contracts match the execution models:

```java
interface ImmediateRateLimiter extends RateLimiter {
    RateLimitDecision tryAcquire(Consumer<RateLimitDecisionEvent> observer);
}

interface QueuedRateLimiter extends RateLimiter {
    Submission submit(Consumer<RateLimitDecisionEvent> observer);
}
```

`RateLimiter` shares only the close lifecycle. Lazy draining and sliding window implement `ImmediateRateLimiter`; bounded queue implements `QueuedRateLimiter`. Only the queue contract exposes a future and cancellation. `FilterConfig` selects the matching HTTP adapter from the selected limiter's admission contract at startup. Concrete filter constructors require the corresponding interface, preventing accidental cross-wiring.

Exactly one rate-limit filter is registered. The filter does not select an algorithm per request. Original requests are charged once; granted async continuations and error dispatches do not acquire again.

## HTTP responses

- 429 with `{"error":"Too many requests"}` for a full bucket, rolling window or waiting queue.
- `Retry-After`: positive seconds rounded upward; another client may consume the next admission first.
- 503 for queue timeout, stopped limiter, unavailable async handling or exceptional queued completion.
- Limiter-generated error responses use `Cache-Control: no-store`.

With default settings, the first ten immediate `/health` requests are eligible and the eleventh is rejected. After six seconds, lazy draining recovers one admission. With sliding window selected, the eleventh must instead wait until the oldest of those ten admissions expires. Health probes share the quota.

## INFO decision logging

Every decision logs a generated request ID, original request time, decision time, HTTP method, configured strategy/capacity/rate, outcome/reason, retry and waiting durations, and an immutable state snapshot. Snapshots are captured atomically with admission; logging and completion callbacks occur outside the state lock. UTC timestamps are diagnostic; rate arithmetic uses monotonic time.

Lazy snapshots describe debt before/after draining and the resulting debt. Sliding-window snapshots describe retained counts, quota and window duration. Queue snapshots describe waiting size, available tokens, token capacity, next-token delay and timeout remaining. Keys state units, such as `Ns`; shared retry/wait fields use milliseconds.

Immediate releases produce one `ALLOWED` event. Waiting queue requests produce `ENQUEUED` followed by a terminal event such as `ALLOWED`, `TIMED_OUT`, `CANCELED` or `STOPPED`. Full queues produce `RATE_LIMITED`. Events share the original request ID; redispatch does not create another admission or log.

Bodies, query strings, tokens, client identities and sensitive headers are excluded. Every decision remains INFO-level; configure log retention for overload traffic.

## Invocation and failure diagnostics

Explicitly declared public methods and constructors in the rate-limit feature and its configuration log INFO entries with `event=method_invocation` and a stable method name. This includes property accessors, interface convenience overloads, submission handles and async listener callbacks. Calls through a convenience overload also invoke and log its implementation. Method arguments, request bodies and headers are not included. Compiler-generated record accessors, `equals`/`hashCode`/`toString`, implicit constructors and inherited framework methods are not instrumented.

Invocation entries are distinct from `event=rate_limit_decision` transitions; filter log consumers by event type when counting admissions. Constructor invocation logging is synchronous and may occur while an immutable snapshot is created inside a state lock. Decision observers, scheduler failure diagnostics and future completion remain outside limiter locks.

Observer failures log WARN with the exception while preserving admission. Scheduler rejection and exceptional queued decisions log ERROR with their exception. Async setup or response/dispatch failures and servlet errors log WARN; timeout is also WARN. An async context already closed during completion is an expected race, logged at DEBUG with the exception. Caught exceptions are not silently discarded. These logs add volume, so size operational retention for the requested INFO invocation policy.

## Testing and tradeoffs

Deterministic tests use manual time and scheduling rather than sleeps. Coverage includes lazy capacities, exact rolling-window expiration, individually staggered admissions, concurrent global admission, immutable snapshots, FIFO, initial token bursts, capped idle refill, partial refill intervals, delayed callbacks, six-second deadlines, cancellation, observer failures, shutdown and scheduler rejection. Filter tests cover immediate rejection, async setup/release races, timeout, metadata, synchronous operation without servlet async support and exceptional queued completion. Factory tests verify defaults, strategy/filter selection and invalid settings. Embedded Tomcat tests exercise global quota, authentication, error responses and actual queued dispatch.

Tests disable the unrelated empty-database startup check. Algorithms hold locks only for short state changes, never while logging, completing futures or invoking HTTP handlers. Spring closes the limiter before destroying its scheduler.

Lazy draining is smallest and permits bursts. Sliding window adds a small timestamp deque to provide the exact rolling guarantee. Token-backed queueing permits bounded bursts and retains servlet resources and introduces timeout/cancellation races; six seconds permits little backlog. A global quota allows one noisy client to consume the allowance. Possible future work includes metrics, overload/cancellation stress tests, explicit health-probe policy and separate concurrency protection. Distributed coordination and per-client fairness remain outside scope.

## Existing seed command limitation

The unrelated empty-database startup check can terminate `make seed` before seeding. A temporary workaround is:

```sh
APP_STARTUP_CHECK=false make seed
```

`make reset` deletes the `data` directory; do not use it against data you want to retain.

## Test directory layout

Unit and integration categories sit directly under `src/test/java`, with production package folders mirrored beneath each category. Class names are unchanged, and Java package declarations match the complete directory path (for example, `unittest.com.weavelab.interview.config`).

```text
src/test/java/
  unittest/com/weavelab/interview/
    config/RateLimitConfigTest.java
    ratelimit/
      RateLimitFilterTest.java
      RateLimitLoggingTest.java
      strategy/impl/
        BoundedQueueTokenTest.java
        ImmediateRateLimiterTest.java
        RateLimitDiagnosticsTest.java
        RateLimiterCoreTest.java
        SlidingWindowRateLimiterTest.java
        ManualScheduler.java
  integrationtest/com/weavelab/interview/
    ApplicationTests.java
    ratelimit/
      DefaultHttpTest.java
      QueueHttpTest.java
      SlidingWindowHttpTest.java
```

`ManualScheduler` is shared unit-test support. Configuration binding tests remain in `unittest`; tests that start an embedded server or the full application belong to `integrationtest`. Both categories continue to run with `./mvnw test`; Maven execution phases are unchanged.
