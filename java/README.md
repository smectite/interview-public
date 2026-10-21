# Global lazy-draining request limiter

This branch implements one process-local, globally shared leaky bucket. All clients and routes share the same allowance. The filter injects the synchronous `ratelimit.strategy.RateLimiter` interface, implemented by the single lazy-draining singleton. There is no strategy selection, queue, scheduler, future, factory or filter-registration configuration class.

## Configuration

```properties
app.rate-limit.capacity=10
app.rate-limit.requests=10
app.rate-limit.period=60s
```

All three settings remain configurable. Any positive capacity is supported. Drainage time per admitted request is `period / requests`: six seconds with the defaults. Period and request count must divide exactly in integer nanoseconds. Nonpositive values, missing durations and arithmetic overflow fail startup. Restart to apply configuration changes; restart resets the bucket. Each application process has independent state.

For example, capacity three, four requests and period two seconds allow an initial burst of three and recover one admission every 500 milliseconds.

## Implementation

The implementation retains the existing package organization:

- `RateLimitProperties`: Spring-bound capacity, requests and period, with startup validation.
- `ratelimit.strategy.impl.LazyDrainingRateLimiter`: the shared Spring singleton and synchronous admission calculation. `ratelimit.strategy.model.RateLimitDecision` carries its immutable state snapshot for diagnostics.
- `ratelimit.RateLimitFilter`: Spring component annotated `@Order(0)`. The existing `AuthFilter` remains a component at `@Order(1)`, so rate limiting runs first. Spring Boot registers the components automatically; no `FilterConfig` is needed.

Each accepted request adds one interval of drain-time debt. On the next attempt, elapsed monotonic time drains that debt, clamped at zero. Admit only when another interval fits under `capacity * interval`. Rejections add no debt. Idle time cannot accumulate credit beyond an empty bucket. A short synchronized section makes the check/update atomic across concurrent callers; request logging and downstream handling occur outside the lock.

The default capacity ten admits ten requests immediately, then recovers one admission every six seconds. It permits bursts and does **not** guarantee at most ten requests in every rolling minute. Capacity one instead enforces six-second spacing with the default rate. Rate limiting controls starts, not the number of slow requests already executing.

## HTTP behavior

An allowed request continues synchronously. A rejected request returns:

- HTTP 429 and JSON `{"error":"Too many requests"}`.
- Positive `Retry-After` seconds, rounded upward. This is advisory under contention.
- `Cache-Control: no-store`.

Health requests, uploads and unauthorized attempts consume the same global allowance. After exhaustion, 429 takes precedence over the authentication interceptor's 401. Downstream errors do not refund admission. `OncePerRequestFilter` skips async and error redispatch by default, so those continuations are not charged again.

## Logging and documentation

Every admission attempt emits an INFO decision entry with request ID, request time, decision time, HTTP method, outcome, retry delay and the exact debt before/after drainage and admission. Snapshots are captured with the state change. Rate arithmetic uses an injectable monotonic ticker; wall-clock timestamps are diagnostic only. Request bodies, paths/query strings, authorization headers and client identities are not logged.

Declared public methods and explicit constructors log INFO invocation entries without arguments. Compiler-generated record methods and inherited framework methods are not instrumented. Invalid configuration and arithmetic failures retain exception evidence; request I/O and downstream failures propagate to the servlet container rather than being swallowed. New production types and public methods have Javadoc.

## Verification

Use Java 17:

```sh
JAVA_HOME=/path/to/jdk17 ./mvnw test
```

Tests use controlled time without sleeps and cover default and arbitrary capacities, custom intervals, exact recovery boundaries, stable snapshots, long idle periods, repeated rejection, concurrent admission, invalid configuration, rounded headers, sensitive-data exclusion, and async/error redispatch. A real embedded servlet test verifies component registration, annotation order, global allowance across clients/routes, and authentication behavior. The existing application context test disables the unrelated empty-database startup check.

### Test organization

Class names are unchanged. `unittest` and `integrationtest` sit directly under `src/test/java`. Package declarations match the complete directory path, for example `unittest.com.weavelab.interview.config`:

```text
src/test/java/
  unittest/com/weavelab/interview/
    config/RateLimitPropertiesTest.java
    ratelimit/RateLimitFilterTest.java
    ratelimit/strategy/impl/LazyDrainingRateLimiterTest.java
  integrationtest/com/weavelab/interview/
    ApplicationTests.java
    ratelimit/RateLimitHttpTest.java
```

`RateLimitPropertiesTest` belongs to `unittest` and uses a small context solely for property binding and validation. Integration tests load the full application or a real servlet server. Both categories continue to run with `./mvnw test`; this change separates their organization, not their Maven lifecycle.
