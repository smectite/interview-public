# Java CRM API

A Java 17 and Spring Boot 3.3 application for contact management, file storage, and activity reporting. Spring MVC handles HTTP requests, `JdbcTemplate` accesses SQLite, and uploaded content resides on the local filesystem. A configurable, process-local rate limiter controls admission before authentication and controller execution.

## Run locally

Use JDK 17 or later and the included Maven wrapper. Run these commands from `java/`:

```sh
# Initialize an empty database with 10,000 contacts and 20 files.
APP_STARTUP_CHECK=false make seed

# Start the HTTP server on port 8080.
make run

# Run unit and integration tests.
./mvnw test
```

The startup check requires a populated database by default. Disable it during initial seeding so it does not exit before the seed command runs. Seeding exits when complete and skips an already populated database. Custom seed counts are supplied as application arguments:

```sh
./mvnw spring-boot:run \
  -Dspring-boot.run.arguments="--app.startup-check=false --app.seed=true --contacts=50000 --files=100"
```

`make build` packages the application with tests skipped. `make reset` and `make clean` delete the local `data/` directory, including the database and uploaded files.

## Architecture and request flow

The application has four principal boundaries:

| Boundary | Responsibility |
| --- | --- |
| HTTP adapters | Route requests, enforce admission, extract identity, and translate decisions into HTTP responses |
| Admission services | Make thread-safe rate decisions using monotonic time, independently of servlet APIs |
| Controllers and repositories | Execute CRM operations, persist records, and record activity |
| Configuration and lifecycle | Validate settings, select one limiter and matching adapter, and manage scheduler shutdown |

An original request follows this path:

```text
HTTP request
  -> selected RateLimitFilter
  -> AuthFilter: extract email identity
  -> AuthInterceptor: require identity for /api/*
  -> controller
  -> repository -> SQLite / local file storage
```

`RateLimitConfig` constructs one shared limiter at startup. `FilterConfig` selects exactly one compatible HTTP adapter and registers it before authentication. Strategy selection is a startup decision, so requests do not repeatedly choose an algorithm.

All routes, HTTP methods, and callers share the same allowance. This includes `/health`, `/api/auth/token`, uploads, and unauthorized requests. Admission can therefore return 429 before authentication would return 401. An admitted request consumes its allowance even if authentication or a downstream operation fails.

The limiter controls how quickly requests start. Already executing requests are outside its accounting. Each application instance owns independent state; restarting the application resets that state.

### Admission contracts

The contracts live in `com.weavelab.interview.ratelimit.service`:

```java
interface IRateLimiter extends AutoCloseable {
    void close();
}

interface IImmediateRateLimiter extends IRateLimiter {
    RateLimitDecision tryAcquire(Consumer<RateLimitDecisionEvent> observer);
}

interface IQueuedRateLimiter extends IRateLimiter {
    ISubmission submit(Consumer<RateLimitDecisionEvent> observer);
}
```

`IImmediateRateLimiter` returns a decision directly. `IQueuedRateLimiter` returns an `ISubmission` exposing a `CompletionStage<RateLimitDecision>`, cancellation, and explicit timeout. `RateLimitDecision` and `RateLimitDecisionEvent` reside in `ratelimit.service.model`; implementations reside in `ratelimit.service.impl`.

`ImmediateRateLimitFilter` depends on the immediate contract. `QueuedRateLimitFilter` owns servlet suspension, timeout listeners, completion, and redispatch. `RateLimitFilter` provides shared request metadata, dispatch guards, and decision handling; `RateLimitResponseWriter` provides the common HTTP error policy. These boundaries keep servlet lifecycle concerns out of the admission algorithms.

## Configuration

Defaults are defined in `src/main/resources/application.properties`. Spring Boot application arguments and environment variables can override them.

| Property | Default | Meaning |
| --- | --- | --- |
| `server.port` | `8080` | HTTP listen port |
| `spring.datasource.url` | `jdbc:sqlite:data/app.db` | SQLite database location |
| `app.data-dir` | `data` | Directory created at initialization; uploaded files use its `files/` subdirectory |
| `app.startup-check` | `true` when unspecified | Require a populated database at startup |
| `app.rate-limit.approach` | `LAZY_DRAINING` | Startup-selected algorithm |
| `app.rate-limit.capacity` | `10` | Lazy-draining burst bound; token and waiting bounds in queue mode |
| `app.rate-limit.requests` | `10` | Admissions per configured period, interpreted by the selected algorithm |
| `app.rate-limit.period` | `60s` | Rate period; exact rolling-window duration for sliding window |
| `app.rate-limit.queue-timeout` | `6s` | Maximum enqueue-relative wait in queue mode |

The refill or drainage interval is `period / requests`, six seconds by default. Application configuration accepts capacity **1 or 10**. Request count and durations must be positive, the period must divide into whole nanosecond intervals, and time calculations must fit supported numeric bounds. Invalid configuration fails startup. Sliding window uses `requests` and `period` for its quota; capacity does not change that quota.

For example, start with bounded queue admission:

```sh
./mvnw spring-boot:run \
  -Dspring-boot.run.arguments="--app.rate-limit.approach=BOUNDED_QUEUE"
```

Changing `app.data-dir` does not change the JDBC URL; configure both when relocating storage. `app.max-upload-size=104857600` is bound by `AppConfig`, but the upload path does not enforce that value. Multipart acceptance also depends on Spring's `spring.servlet.multipart.max-file-size` and `spring.servlet.multipart.max-request-size` settings; this application does not explicitly configure those settings.

## Rate-limiting strategies

| Strategy | State | Admission policy | Default behavior |
| --- | --- | --- | --- |
| `LAZY_DRAINING` | Drain-time debt | Admit immediately when one more interval fits within capacity; otherwise reject | Burst of ten, then one recovered admission every six seconds |
| `SLIDING_WINDOW` | Deque of accepted timestamps | Admit only when fewer than `requests` accepted timestamps remain in the rolling period | At most ten admissions in any rolling minute |
| `BOUNDED_QUEUE` | Token bucket and bounded FIFO queue | Consume an available token, wait for one, or reject a full queue | Ten initial tokens, up to ten waiting entries, six-second refill and waiting timeout |

### Lazy draining

Each admission adds one interval of debt. On each attempt, elapsed monotonic time reduces debt, clamped at zero. Admission succeeds when adding another interval would stay within `capacity * interval`. Rejections add no debt. The algorithm needs constant state and performs no scheduled work.

This policy supports bursts and a sustained recovery rate. With default settings it can accept ten requests at time zero and another at six seconds. It does not enforce ten requests per rolling minute. Capacity one provides interval spacing between admissions.

### Sliding window

Under one lock, discard accepted timestamps whose age is at least `period`, then either append the current timestamp or reject a full window. Rejections do not extend the window. The retained interval is `(now - period, now]`; an admission exactly one period old falls outside the window.

Memory is bounded by `requests`. Expiration occurs on admission attempts without a scheduler. A rejection's retry duration is the time until the oldest retained admission leaves the window. This is the strategy for an exact rolling quota.

### Bounded queue

`BoundedQueueRateLimiter` starts with `capacity` tokens. Each admitted request consumes one token. If no older request is waiting, a new request can consume a token immediately. Otherwise it joins the FIFO queue, provided that queue has space. Capacity independently bounds stored tokens and waiting entries; neither bound counts requests already admitted and executing.

Processing follows a fixed order: refill tokens, expire overdue entries, then release surviving entries from the queue head. A new submission processes existing waiters before considering its own admission. New arrivals therefore cannot bypass older waiting requests.

Refill earns one token per whole interval, capped at capacity. Partial intervals are retained while the bucket remains below capacity. Once full, excess idle credit is discarded. A late scheduler callback may release multiple entries using accumulated tokens, so releases can occur in bursts.

Each waiting entry has a deadline measured from submission. Expiration wins when that deadline coincides with token availability. With default settings and all tokens spent at time zero:

- A request queued at time zero expires at six seconds, before spending the token earned then.
- A request queued at time one can use that token at time six after waiting five seconds.

Queue space therefore does not guarantee eventual admission. Cancellation and timeout remove pending entries without consuming tokens. An already granted admission cannot be undone or refunded.

The scheduler wakes at the earlier of the next token or earliest waiting deadline. It schedules work only while entries wait. Scheduler rejection permanently closes admission and settles pending entries as STOPPED. Spring closes the limiter before shutting down the externally owned executor.

### Concurrency and notification delivery

Each limiter synchronizes state transitions and diagnostic snapshots under one monitor. Monotonic time drives rate calculations; UTC wall-clock time is used for diagnostic timestamps.

Observer delivery and future completion occur after releasing the state lock. Each queued entry has an ordered notification mailbox and atomic drain guard, preserving ENQUEUED before its terminal event even when several threads compete to deliver notifications. Terminal state is chosen under the limiter lock, so release, timeout, cancellation, and shutdown settle each attempt once.

Callbacks and synchronous completion-stage continuations run on the thread delivering the result. They should return promptly. Invocation logging during snapshot construction can occur inside the state lock; deferred decision delivery does not imply that all logging occurs outside it.

### Servlet async lifecycle

A pending admission starts Servlet async processing and returns the request thread to the container. The adapter installs timeout and error listeners before attaching the completion callback. An atomic completion guard arbitrates the response and dispatch races.

An ALLOWED result marks the request as granted and calls `AsyncContext.dispatch()`. Authentication and controller execution resume on a container thread. The granted redispatch does not acquire another allowance. Error dispatches also do not charge admission again.

Servlet timeout signals TIMED_OUT; error or disconnect signals cancellation of pending admission. Disconnect detection depends on the container. Async setup failures cancel pending work and return 503. Clients and proxies must allow enough time for the configured queue wait as well as application execution.

## HTTP contract

| Condition | Response |
| --- | --- |
| Admission allowed | Continue to authentication and the requested handler |
| Bucket, rolling window, or waiting queue full | 429, `{"error":"Too many requests"}`, and `Retry-After` |
| Queue deadline elapsed | 503, `{"error":"Rate limit queue timeout"}` |
| Limiter stopped or queued decision failed | 503, `{"error":"Rate limiter unavailable"}` |
| Async processing unavailable | 503, `{"error":"Asynchronous processing unavailable"}` |
| Missing or invalid identity for an admitted `/api/*` request | 401, `{"error":"Unauthorized"}` |

Limiter-generated error responses use JSON, UTF-8, and `Cache-Control: no-store`. `Retry-After` rounds upward to positive whole seconds. It is advisory: another caller may take the next available admission.

### Identity and endpoints

Send an email address as the bearer token:

```sh
curl -H 'Authorization: Bearer user@example.com' \
  'http://localhost:8080/api/contacts?limit=10'
```

`AuthFilter` validates the email format and exposes it as the request's user identity. `AuthInterceptor` requires that identity for every `/api/*` route, including `/api/auth/token`. The token endpoint confirms the supplied identity; it does not issue a signed credential. Repository queries access shared records rather than filtering data by caller identity. Activity records use the identity for attribution.

| Method | Path | Behavior |
| --- | --- | --- |
| GET | `/health` | Health response; identity optional, rate admission required |
| POST | `/api/auth/token` | Return authenticated identity and status |
| GET | `/api/contacts` | Cursor-paginated contacts; default limit 50, valid range 1–100 |
| GET | `/api/contacts/{id}` | Read one contact |
| POST | `/api/contacts` | Create a contact |
| PUT | `/api/contacts/{id}` | Update a contact |
| DELETE | `/api/contacts/{id}` | Delete a contact |
| POST | `/api/contacts/import` | Import a JSON array of at most 10,000 contacts |
| GET | `/api/contacts/export` | Export contacts as CSV |
| GET | `/api/files` | List file metadata |
| POST | `/api/files` | Upload multipart field `file` |
| GET | `/api/files/{id}` | Download file content |
| GET | `/api/reports/activity` | Activity report; optional `since=YYYY-MM-DD`, default last 30 days |

Contact pagination orders by `created_at DESC, id DESC`. The response supplies `next_page_token` when another page exists; pass it back as `page_token`. Invalid page tokens return 400. Limits outside 1–100 fall back to 50.

## Persistence and consistency

`DataSourceConfig` creates the data and file directories and applies `schema.sql` during initialization. SQLite stores contacts, file metadata, and activity records. File content is stored under generated IDs in `app.data-dir/files`.

`FileRepository` removes partially written content after an I/O failure and removes content if metadata insertion fails. The filesystem and SQLite remain separate persistence boundaries. CRM writes and subsequent activity logging are separate repository operations, and contact import performs individual inserts without an enclosing transaction. A failure can therefore leave an operation partially applied. Contact export loads all matching contacts before writing CSV; it is not a bounded-memory database stream.

## Observability

Admission transitions log at INFO with `event=rate_limit_decision`. Each event includes a generated request ID, original request time, decision time, HTTP method, strategy and configured rate, outcome and reason, retry and waiting durations, and an immutable state snapshot. Bodies, query strings, bearer tokens, and caller identities are excluded from these decision events.

Immediate attempts emit a terminal decision. Waiting attempts emit ENQUEUED followed by ALLOWED, TIMED_OUT, CANCELED, or STOPPED. A full queue emits RATE_LIMITED. Events for one attempt retain the same request ID. Snapshot fields describe debt, rolling-window occupancy, or queue/token state; time fields identify their units.

Instrumented public methods and constructors in the rate-limit feature and configuration also emit INFO records with `event=method_invocation`. Filter by event type when counting admissions. Observer failures log WARN and preserve the selected decision. Scheduler rejection and exceptional queued completion log ERROR; servlet timeout and async failures log WARN. An already completed async context is logged at DEBUG. Log retention and throughput must accommodate decision and invocation volume during overload.

## Tests and operating boundaries

`src/test/java/unittest` contains algorithm, configuration, filter, and logging tests. `src/test/java/integrationtest` contains application and embedded HTTP tests. Both categories run in the Maven test phase with `./mvnw test`.

Algorithm tests inject manual time and scheduling to exercise refill, expiration boundaries, FIFO ordering, cancellation, concurrent admission, and shutdown. Filter tests exercise response mapping and async races. Embedded Tomcat tests exercise shared quotas, authentication, and queued redispatch. Tests disable the populated-database startup check where necessary.

The deployment boundary is one application process with local database and file storage. The global quota offers no per-client isolation and no coordination across replicas. Queue mode bounds waiting entries while retaining servlet resources for each waiter. Rate limits provide admission control; resource sizing must also account for execution time, concurrent handlers, database access, uploads, and response buffering.
