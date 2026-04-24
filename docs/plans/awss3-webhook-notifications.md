# Implementation Plan — `awss3exporter` HTTP webhook notifications

**Spec:** [docs/superpowers/specs/2026-04-24-awss3-webhook-notifications-design.md](../superpowers/specs/2026-04-24-awss3-webhook-notifications-design.md)
**Component:** `exporter/awss3exporter`
**Target branch:** `design/awss3-webhook-notifications`

## Overview

Add optional HTTP webhook notifications to `exporter/awss3exporter`. When
`s3uploader.notifications.endpoint` is non-empty, the exporter POSTs an
S3-event-shaped JSON envelope to that endpoint after every successful S3
upload. Delivery uses a bounded in-memory queue (drop-on-full), a shared
worker pool, size-triggered batching, exponential-backoff retries, and
graceful drain on shutdown. Hand-rolled metrics are emitted for send
success, drops (by reason), and per-attempt latency (by status class).

The feature is disabled by default and is a pure additive change; the
noop path keeps existing tests and behavior unaffected.

## File Changes

### New files

| Path | Purpose |
|---|---|
| `exporter/awss3exporter/internal/notify/config.go` | `NotificationsConfig` struct, defaults helper, `Validate()`. |
| `exporter/awss3exporter/internal/notify/notifier.go` | `Notifier` interface, `Event` struct, `New(...)` factory, `noopNotifier`, live `httpNotifier` implementation (queue, workers, batching, retry, shutdown). |
| `exporter/awss3exporter/internal/notify/payload.go` | S3-event envelope types + `marshalBatch([]Event, time.Time) ([]byte, error)` with URL-encoded keys. |
| `exporter/awss3exporter/internal/notify/metrics.go` | Hand-rolled instruments (`sent` counter, `dropped` counter, `send.duration` histogram) and helpers that wrap increments so callers don't touch `metric.WithAttributes` directly. |
| `exporter/awss3exporter/internal/notify/config_test.go` | Table tests for `NotificationsConfig.Validate()` and defaults. |
| `exporter/awss3exporter/internal/notify/payload_test.go` | Tests for marshal shape + URL-encoding. |
| `exporter/awss3exporter/internal/notify/notifier_test.go` | Unit tests 1–11 from the spec (see Testing Strategy). |
| `exporter/awss3exporter/testdata/config-notifications.yaml` | Fixture used by `TestLoadConfig`-style test for the notifications block. |

### Modified files

| Path | Change |
|---|---|
| `exporter/awss3exporter/config.go` | Add `Notifications notify.NotificationsConfig \`mapstructure:"notifications"\`` to `S3UploaderConfig`; add notifications validation to `Config.Validate()`. |
| `exporter/awss3exporter/config.schema.yaml` | Regenerate/update schema so the new `notifications` block appears in checked-in config docs and schema-based tests stay aligned. |
| `exporter/awss3exporter/factory.go` | Add `exporterhelper.WithShutdown(s3Exporter.shutdown)` to traces/logs/metrics exporter constructions; include the default `NotificationsConfig` in `createDefaultConfig()`. |
| `exporter/awss3exporter/exporter.go` | Add `notifier notify.Notifier` field; construct it in `start(ctx, host)` via `notify.New(...)`; pass it through to the upload manager; add `shutdown(ctx) error` that calls `notifier.Shutdown(ctx)`. |
| `exporter/awss3exporter/s3_writer.go` | Accept a `notify.Notifier` in `newUploadManager`; pass it through `upload.WithNotifier(...)`. |
| `exporter/awss3exporter/internal/upload/writer.go` | Add upload-local notifier adapter types (`EventNotifier`, `NotifyEvent`), add `notifier` field + `WithNotifier`, and call `sw.notifier.Enqueue(...)` after a successful `UploadObject`. |
| `exporter/awss3exporter/internal/upload/writer_test.go` | Update `NewS3Manager` call sites if signature changes; add a subtest asserting `Enqueue` is called on a stub notifier with the right `(bucket, key, size)`. |
| `exporter/awss3exporter/config_test.go` | Add `TestConfigNotifications` that loads `testdata/config-notifications.yaml` and asserts the decoded struct. Extend `TestConfig_Validate` with a few notifications-specific rows. |
| `exporter/awss3exporter/go.mod` / `go.sum` | Add `go.opentelemetry.io/collector/config/confighttp` (already used elsewhere in the repo at matching version). Run `go mod tidy`. |

### No deletes.

Do not hand-edit `internal/metadata/generated_status.go`; it should stay
unchanged. Keep any schema regeneration/update diff limited to the new
config surface; do not let it pull unrelated generated-file churn into
the PR.
The existing `ScopeName` constant remains the meter scope for the new
instruments (see spec: preserve telemetry continuity with upstream).

## Implementation Steps

Steps are ordered so each one compiles and passes its own tests before
the next builds on it. Do not reorder without reading the dependency
notes.

### Step 1 — Ensure `confighttp` is a direct dependency

**Files:** `exporter/awss3exporter/go.mod`

- Add `go.opentelemetry.io/collector/config/confighttp v0.150.0` as a
  direct requirement if it is not already present in the module's
  `require` block. Do not version-hunt here; this repo already uses
  `v0.150.0` for `confighttp` in sibling modules and the checksum is
  already present in `exporter/awss3exporter/go.sum`.
- Run `cd exporter/awss3exporter && go mod tidy`.
- No code uses it yet; the module should still build.

**Verification:** `cd exporter/awss3exporter && go build ./...`

### Step 2 — `notify.Event`, `Notifier` interface, `noopNotifier`

**Files:** `exporter/awss3exporter/internal/notify/notifier.go` (new)

- Package declaration and doc comment.
- Apache 2.0 license header (copy from `exporter/awss3exporter/exporter.go`).
- Define:
  ```go
  type Event struct {
      Bucket string
      Key    string // raw, un-escaped; notifier escapes at marshal time
      Size   int64  // post-compression byte length
  }

  type Notifier interface {
      Enqueue(ctx context.Context, e Event) bool
      Shutdown(ctx context.Context) error
  }
  ```
- Define `noopNotifier struct{}`:
  - `Enqueue` returns `false` (signals "not accepted" so callers that
    care can branch; the upload manager ignores the return).
  - `Shutdown(ctx context.Context) error` returns `nil` immediately.
- Export `func NewNoop() Notifier { return noopNotifier{} }` for tests.
- The live notifier type and `New(...)` factory are added in Step 6 —
  stub `New` now so the package compiles:
  ```go
  func New(cfg Config, scopeName string, telemetry component.TelemetrySettings, host component.Host, logger *zap.Logger) (Notifier, error) {
      if cfg.Endpoint == "" {
          return noopNotifier{}, nil
      }
      return nil, errors.New("live notifier not yet implemented")
  }
  ```
  (Replaced in Step 6 — the error branch is unreachable in tests until
  then because no test sets `Endpoint`.)

**Verification:** `cd exporter/awss3exporter && go build ./internal/notify/...`

### Step 3 — `NotificationsConfig` and validation

**Files:** `exporter/awss3exporter/internal/notify/config.go`,
`exporter/awss3exporter/internal/notify/config_test.go`

- Define `Config` (exported, referenced by `S3UploaderConfig` as
  `notify.Config`):
  ```go
  type Config struct {
      confighttp.ClientConfig `mapstructure:",squash"`

      QueueSize         int           `mapstructure:"queue_size"`
      Workers           int           `mapstructure:"workers"`
      MaxRecordsPerPost int           `mapstructure:"max_records_per_post"`
      MaxAttempts       int           `mapstructure:"max_attempts"`
      InitialBackoff    time.Duration `mapstructure:"initial_backoff"`
      MaxBackoff        time.Duration `mapstructure:"max_backoff"`
  }
  ```
- Export `NewDefaultConfig()`:
  ```go
  func NewDefaultConfig() Config {
      cc := confighttp.NewDefaultClientConfig()
      cc.Timeout = 10 * time.Second
      return Config{
          ClientConfig:      cc,
          QueueSize:         10000,
          Workers:           4,
          MaxRecordsPerPost: 100,
          MaxAttempts:       3,
          InitialBackoff:    time.Second,
          MaxBackoff:        30 * time.Second,
      }
  }
  ```
- `func (c *Config) Validate() error`: when `c.Endpoint == ""`, return
  `nil` (feature off). Otherwise check in this order, accumulating with
  `multierr.Append`:
  1. `c.ClientConfig.Validate()` — delegates to confighttp.
  2. Parse `c.Endpoint` with `url.Parse`; scheme must be `http` or
     `https`; return `fmt.Errorf("notifications.endpoint must be http(s) URL: %q", c.Endpoint)` on failure.
  3. Reject reserved request headers in `c.Headers` using
     case-insensitive matching:
     - `Content-Type` must not be user-configurable, because the
       notifier must always send `application/json` and `confighttp`
       header middleware overwrites existing request header values.
     - `Content-Encoding` must not be user-configurable.
     Return stable errors
     `notifications.headers must not override Content-Type` and
     `notifications.headers must not override Content-Encoding`.
  4. Reject request-body compression for notifications:
     `c.Compression` must be empty / uncompressed. Return
     `notifications.compression is not supported`.
     Rationale: `confighttp` would otherwise gzip/zstd the webhook body,
     but the receiver contract in the spec is plain JSON POST and this
     design does not define compressed request handling.
  5. `c.QueueSize >= 1`, else `errors.New("notifications.queue_size must be >= 1")`.
  6. `c.Workers >= 1`, else `errors.New("notifications.workers must be >= 1")`.
  7. `c.MaxRecordsPerPost >= 1`, else analogous error.
  8. `c.MaxAttempts >= 1`, else analogous error.
  9. `c.InitialBackoff > 0`, else analogous error.
  10. `c.MaxBackoff >= c.InitialBackoff`, else `errors.New("notifications.max_backoff must be >= initial_backoff")`.
- Table test `TestConfigValidate` covers: empty endpoint (pass), bad
  scheme, missing scheme, reserved `Content-Type` header, reserved
  `Content-Encoding` header, compressed request body configured,
  negative queue size, zero workers, zero max_records, zero attempts,
  zero initial backoff, max_backoff < initial_backoff, happy path, and
  "endpoint empty ignores otherwise-invalid notification settings".
  Assert error text is stable (use `assert.ErrorContains` to avoid
  brittleness on ordering when multierr combines).

**Verification:** `cd exporter/awss3exporter && go test ./internal/notify/...`

### Step 4 — Payload marshaling

**Files:** `exporter/awss3exporter/internal/notify/payload.go`,
`exporter/awss3exporter/internal/notify/payload_test.go`

- Define unexported JSON types mirroring the S3 event shape. Use
  struct tags to produce exact field names the spec lists
  (`eventSource`, `eventName`, `eventTime`, `Records`, `s3`,
  `bucket.name`, `object.key`, `object.size`). `Records` is the only
  capitalized field in the JSON because that matches AWS's S3 shape.
  ```go
  type s3Envelope struct {
      Records []s3Record `json:"Records"`
  }
  type s3Record struct {
      EventSource string   `json:"eventSource"`
      EventName   string   `json:"eventName"`
      EventTime   string   `json:"eventTime"`
      S3          s3Entity `json:"s3"`
  }
  type s3Entity struct {
      Bucket s3Bucket `json:"bucket"`
      Object s3Object `json:"object"`
  }
  type s3Bucket struct { Name string `json:"name"` }
  type s3Object struct {
      Key  string `json:"key"`
      Size int64  `json:"size"`
  }
  ```
- Constants: `eventSource = "aws:s3"`, `eventName = "ObjectCreated:Put"`.
- `marshalBatch(events []Event, now time.Time) ([]byte, error)`:
  - Format `now.UTC().Format(time.RFC3339)`. Same timestamp for every
    record in the batch — this reflects "the moment the notifier
    serialized the batch", which is acceptable per spec.
  - `object.key = url.QueryEscape(e.Key)`.
  - `object.size = e.Size` verbatim. Bucket name verbatim.
  - Return `json.Marshal(envelope)`.
- Test `TestMarshalBatch`:
  - Single event: assert exact JSON structure via a decode + field
    checks (do not compare raw bytes — map ordering is stable for
    structs but the test should read as intent).
  - URL encoding: key `raw/plus+char file.json` → marshalled body
    contains `raw/plus%2Bchar%20file.json`.
  - Multi-event batch: 3 events → 3 records, each with distinct keys.
  - `eventTime` matches RFC3339 UTC; use a fixed `now` in the test.

**Verification:** `cd exporter/awss3exporter && go test ./internal/notify/...`

### Step 5 — Metrics

**Files:** `exporter/awss3exporter/internal/notify/metrics.go`

- Constants for instrument names (strings match spec exactly):
  - `notifications.sent`
  - `notifications.dropped`
  - `notifications.send.duration`
- Define `type instruments struct { sent metric.Int64Counter; dropped metric.Int64Counter; duration metric.Float64Histogram }`.
- `newInstruments(meter metric.Meter) (*instruments, error)`:
  - `sent`: `meter.Int64Counter("notifications.sent", metric.WithDescription("Successfully delivered S3 upload notifications, counted per record."))`
  - `dropped`: `meter.Int64Counter("notifications.dropped", metric.WithDescription("Notifications that did not reach the receiver, counted per record."))`
  - `duration`: `meter.Float64Histogram("notifications.send.duration", metric.WithDescription("Per-attempt HTTP POST duration, in seconds."), metric.WithUnit("s"))`
- Helpers:
  - `recordSent(ctx, n int64)` — always uses attr `outcome=success`.
  - `recordDropped(ctx, n int64, reason string)` — `reason` is one of
    `queue_full | permanent_4xx | retries_exhausted | shutdown`.
    Declare the four reason strings as exported constants in this file
    so callers don't typo them.
  - `recordDuration(ctx, d time.Duration, statusClass string)` —
    status class is one of `2xx | 4xx | 5xx | network_error`.
    Similarly, four exported constants.
- Meter scope: caller passes in `metadata.ScopeName` (via the
  notifier's `New`). Do not import `internal/metadata` here — keep
  this package self-contained and let callers wire the meter.

**Verification:** `cd exporter/awss3exporter && go build ./internal/notify/...`

### Step 6 — Live `httpNotifier` (queue + workers + batching + retry + shutdown)

**Files:** `exporter/awss3exporter/internal/notify/notifier.go` (replace
Step 2's `New` stub), small helpers may live in the same file.

#### Struct

```go
type httpNotifier struct {
    cfg       Config
    client    *http.Client
    logger    *zap.Logger
    instr     *instruments

    ch        chan Event
    stopCh    chan struct{}
    accepting atomic.Bool

    shutdownCtx    context.Context
    cancelShutdown context.CancelFunc

    wg sync.WaitGroup

    // for deterministic jitter in tests
    rand func() float64
}
```

#### `New(cfg Config, scopeName string, telemetry component.TelemetrySettings, host component.Host, logger *zap.Logger) (Notifier, error)`

1. If `cfg.Endpoint == ""`, return `noopNotifier{}`.
2. `client, err := cfg.ClientConfig.ToClient(context.Background(), host.GetExtensions(), telemetry)`. On error, return
   `nil, fmt.Errorf("notifications: build http client: %w", err)`.
   - `ToClient` wraps configured headers at the transport layer, which
     is why Step 3 forbids users from supplying `Content-Type` or
     `Content-Encoding`.
3. `instr, err := newInstruments(telemetry.MeterProvider.Meter(scopeName))`. On error, return it (telemetry wiring bugs should surface at startup).
   - `scopeName` is passed in from the exporter as `metadata.ScopeName`
     so `internal/notify` stays decoupled from `internal/metadata`.
4. Build struct:
   - `ch = make(chan Event, cfg.QueueSize)`.
   - `stopCh = make(chan struct{})`.
   - `shutdownCtx, cancelShutdown = context.WithCancel(context.Background())`.
   - `accepting.Store(true)`.
   - `rand = func() float64 { return mathrand.Float64() }` (seeded globally; tests can override via an unexported constructor).
5. Start `cfg.Workers` goroutines calling `n.workerLoop()`, but make the
   wait-group increment happen in the parent goroutine before any `go`
   statement so `Shutdown` can never observe a zero-count wait group
   while workers are still being launched. Either of these forms is
   acceptable:
   ```go
   n.wg.Add(cfg.Workers)
   for i := 0; i < cfg.Workers; i++ {
       go n.workerLoop()
   }
   ```
   or
   ```go
   for i := 0; i < cfg.Workers; i++ {
       n.wg.Add(1)
       go n.workerLoop()
   }
   ```
   Do not call `wg.Add(1)` inside `workerLoop()`.
6. Return `&httpNotifier{...}`.

#### `Enqueue(ctx context.Context, e Event) bool`

- Fast path drop if `!n.accepting.Load()`: record `dropped{reason=shutdown}=1`, return `false`.
- Respect caller cancellation: `if err := ctx.Err(); err != nil { return false }` (do not count; this is the caller going away, not a queue problem).
- Non-blocking send:
  ```go
  select {
  case n.ch <- e:
      return true
  default:
      n.instr.recordDropped(ctx, 1, reasonQueueFull)
      return false
  }
  ```

#### `workerLoop()`

```go
defer n.wg.Done()
for {
    select {
    case <-n.stopCh:
        n.drain()
        return
    case first, ok := <-n.ch:
        if !ok { return }
        batch := make([]Event, 0, n.cfg.MaxRecordsPerPost)
        batch = append(batch, first)
        for len(batch) < n.cfg.MaxRecordsPerPost {
            select {
            case e := <-n.ch:
                batch = append(batch, e)
            default:
                goto flush
            }
        }
    flush:
        n.postBatch(n.shutdownCtx, batch)
    }
}
```

Notes:
- The `goto flush` pattern matches the spec's pseudocode exactly.
- `postBatch` uses `n.shutdownCtx` so in-flight POSTs can be canceled
  when shutdown's deadline fires.

#### `drain()`

On `stopCh`:
```go
for {
    batch := make([]Event, 0, n.cfg.MaxRecordsPerPost)
    // drain up to maxBatch non-blockingly
    for len(batch) < n.cfg.MaxRecordsPerPost {
        select {
        case e := <-n.ch:
            batch = append(batch, e)
        default:
            goto checkEmpty
        }
    }
checkEmpty:
    if len(batch) == 0 {
        return
    }
    if n.shutdownCtx.Err() != nil {
        n.instr.recordDropped(context.Background(), int64(len(batch)), reasonShutdown)
        return
    }
    n.postBatch(n.shutdownCtx, batch)
}
```

Clarify the counting rule so it's unambiguous:
- A worker counts `shutdown` drops only for batches it has *already
  pulled* and is choosing not to send because `shutdownCtx` is done.
- After all workers return (via `wg`), `Shutdown` does one final
  non-blocking drain of `ch` in the main goroutine and counts any
  remaining events as `shutdown` drops. This is correct even if some
  other worker exited earlier: once `wg.Wait()` has completed there are
  no worker readers left, so the final sweep is the sole consumer of
  any leftover channel entries.
- Therefore there is no double counting:
  - events already removed from `ch` by a worker are either sent or
    counted by that worker
  - events still present in `ch` after every worker exits are counted
    exactly once by `Shutdown`
- There is also no missed accounting window between a worker's last
  drain attempt and the final sweep, because producers are shut off by
  `accepting.CompareAndSwap(true, false)` before `stopCh` is closed, so
  no new events can enter `ch` once draining begins.

#### `postBatch(ctx context.Context, batch []Event)`

Outer retry loop over `n.cfg.MaxAttempts`:

```go
body, err := marshalBatch(batch, time.Now())
if err != nil {
    n.logger.Warn("marshal notification batch", zap.Error(err))
    n.instr.recordDropped(ctx, int64(len(batch)), reasonRetriesExhausted) // surface as drop; unreachable unless json.Marshal panics on our own types
    return
}

for attempt := 0; attempt < n.cfg.MaxAttempts; attempt++ {
    if ctx.Err() != nil {
        n.instr.recordDropped(context.Background(), int64(len(batch)), reasonShutdown)
        return
    }
    statusClass, permanent, retriable, err := n.doOnePost(ctx, body)
    // doOnePost records duration + status_class before returning
    switch {
    case err == nil:
        n.instr.recordSent(ctx, int64(len(batch)))
        return
    case permanent:
        n.logger.Warn("notification permanent failure",
            zap.String("statusClass", statusClass),
            zap.String("firstKey", batch[0].Key),
            zap.String("firstBucket", batch[0].Bucket))
        n.instr.recordDropped(ctx, int64(len(batch)), reasonPermanent4xx)
        return
    case !retriable:
        // should not happen; defensive
        n.instr.recordDropped(ctx, int64(len(batch)), reasonRetriesExhausted)
        return
    }
    // retriable: sleep before next attempt unless this was the last
    if attempt+1 == n.cfg.MaxAttempts {
        n.instr.recordDropped(ctx, int64(len(batch)), reasonRetriesExhausted)
        return
    }
    sleep := n.backoff(attempt)
    select {
    case <-time.After(sleep):
    case <-ctx.Done():
        n.instr.recordDropped(context.Background(), int64(len(batch)), reasonShutdown)
        return
    }
}
```

#### `doOnePost(parentCtx, body) (statusClass string, permanent bool, retriable bool, err error)`

1. `attemptCtx, cancel := context.WithTimeout(parentCtx, n.cfg.ClientConfig.Timeout)`. `defer cancel()`.
2. Build `req, err := http.NewRequestWithContext(attemptCtx, http.MethodPost, n.cfg.Endpoint, bytes.NewReader(body))`. If error, return `("network_error", false, true, err)` (URL built during Validate; errors here indicate programmer error but are safe to treat as retriable).
3. `req.Header.Set("Content-Type", "application/json")`. (Other headers come from `confighttp.ClientConfig.Headers` via `ToClient`; confighttp applies them transparently through the transport, so no extra code is needed here.)
   - Because validation forbids `notifications.headers.Content-Type`,
     this remains the final `Content-Type` on the wire.
4. `start := time.Now()`. `resp, err := n.client.Do(req)`. `elapsed := time.Since(start)`.
5. On `err != nil`:
   - `n.instr.recordDuration(parentCtx, elapsed, statusNetworkError)`.
   - Return `(statusNetworkError, false, true, err)`.
6. `defer resp.Body.Close()`. Drain body: `io.Copy(io.Discard, resp.Body)`.
7. Classify:
   - `2xx`: `recordDuration(..., status2xx)`; return `(status2xx, false, false, nil)`.
   - `4xx`: `recordDuration(..., status4xx)`; return `(status4xx, true, false, fmt.Errorf("http %d", resp.StatusCode))`.
   - `5xx`: `recordDuration(..., status5xx)`; return `(status5xx, false, true, fmt.Errorf("http %d", resp.StatusCode))`.
   - Anything else (1xx/3xx): treat as retriable; `recordDuration(..., status5xx)` (best bucket) and return `(status5xx, false, true, ...)`.

#### `backoff(attempt int) time.Duration`

```go
base := n.cfg.InitialBackoff * (1 << attempt)
if base > n.cfg.MaxBackoff { base = n.cfg.MaxBackoff }
jitter := 0.5 + n.rand() // [0.5, 1.5)
return time.Duration(float64(base) * jitter)
```

Independent `n.rand()` per attempt = independent jitter, which prevents
the thundering-herd described in the spec.

#### `Shutdown(ctx context.Context) error`

```go
if !n.accepting.CompareAndSwap(true, false) {
    return nil // idempotent
}
close(n.stopCh)

// Wait for workers bounded by ctx.
done := make(chan struct{})
go func() { n.wg.Wait(); close(done) }()
select {
case <-done:
case <-ctx.Done():
    n.cancelShutdown() // unblock in-flight POSTs
    <-done             // workers must return now
}

// Final sweep: drain any leftovers.
for {
    select {
    case <-n.ch:
        n.instr.recordDropped(context.Background(), 1, reasonShutdown)
    default:
        return nil
    }
}
```

- The channel is never closed (spec requirement — producers must not
  observe close races).
- `cancelShutdown` cancels the ctx that `postBatch` / `doOnePost` use,
  so in-flight HTTP calls abort promptly.

**Verification:** `cd exporter/awss3exporter && go test -race ./internal/notify/...`

### Step 7 — Wire `S3UploaderConfig`, defaults, validation, and schema

**Files:** `exporter/awss3exporter/config.go`, `exporter/awss3exporter/factory.go`, `exporter/awss3exporter/config.schema.yaml`

- Import `notify "github.com/cardinalhq/cardinalhq-otel-collector/exporter/awss3exporter/internal/notify"`.
- Add `Notifications notify.Config \`mapstructure:"notifications"\`` at the end of `S3UploaderConfig`.
- In `Config.Validate()`, after the existing checks, append `multierr.Append(errs, c.S3Uploader.Notifications.Validate())`.
- In `createDefaultConfig()`, set `S3Uploader.Notifications: notify.NewDefaultConfig()` so defaults round-trip through mapstructure. This keeps the "feature off" state (empty endpoint) intact — none of the populated defaults are load-bearing when endpoint is empty.
- Regenerate/update `config.schema.yaml` after the struct change so the
  checked-in schema includes the nested `notifications` block and the
  squashed `confighttp` fields that are intentionally exposed there.
  In this workspace, `cd exporter/awss3exporter && make mdatagen` is not
  available because the included shared makefile is missing, so do not
  rely on that command in implementation or verification. Update the
  checked-in schema file directly or via whatever generation path the
  branch/repo actually provides, and keep the diff limited to the new
  config surface.

**Verification:**
- `cd exporter/awss3exporter && go test -race -run '^(TestLoadConfig|TestConfig|TestConfigNotifications|TestConfig_Validate)$' ./...`
- Existing tests must still pass. `TestLoadConfig` compares against an
  expected `*Config`; if adding the `Notifications: notify.NewDefaultConfig()`
  field changes equality, update the expected struct in the same PR.
- Inspect the diff and confirm `internal/metadata/generated_status.go`
  is unchanged.

### Step 8 — Upload manager: inject notifier, fire `Enqueue`

**Files:** `exporter/awss3exporter/internal/upload/writer.go`, `exporter/awss3exporter/internal/upload/writer_test.go`

- Add an interface to the `upload` package to avoid an import cycle
  (upload must not import `internal/notify`):
  ```go
  // EventNotifier is the subset of notify.Notifier the upload manager needs.
  type EventNotifier interface {
      Enqueue(ctx context.Context, e NotifyEvent) bool
  }
  type NotifyEvent struct {
      Bucket string
      Key    string
      Size   int64
  }
  ```
  The `s3Exporter` will adapt `notify.Notifier` → `EventNotifier` via
  a small shim (or — simpler — keep the `notify.Event` and
  `notify.Notifier` types here and have `internal/notify` re-export
  them via type aliases). **Decision:** use the adapter pattern.
  Define `EventNotifier` and `NotifyEvent` in `upload` so
  `internal/upload` has zero dependency on `internal/notify`.
- Add field `notifier EventNotifier` to `s3manager`. Default to a
  package-private `noopEventNotifier{}` when not set, so
  `sw.notifier.Enqueue(...)` is always safe.
- Add `func WithNotifier(n EventNotifier) ManagerOpt`.
- In `Upload(...)`, after the successful `sw.uploader.UploadObject(...)`:
  ```go
  sw.notifier.Enqueue(ctx, NotifyEvent{
      Bucket: overrideBucket,
      Key:    key,
      Size:   int64(content.Len()),
  })
  ```
  The return value is intentionally ignored (per spec).
- In `writer_test.go`:
  - Add a stub `type stubNotifier struct { mu sync.Mutex; got []NotifyEvent }` with `Enqueue`.
  - Add one subtest `"notifies after successful upload"` that injects
    the stub via `WithNotifier`, performs a successful upload, and
    asserts `stub.got` has one entry with the expected bucket, key,
    and size (size = compressed bytes for compression cases — verify
    for at least one gzip case).
  - Do not add a notifier for the existing subtests; they exercise
    the default `noopEventNotifier`.

**Verification:** `cd exporter/awss3exporter && go test -race ./internal/upload/...`

### Step 9 — Exporter wiring: build + inject notifier in `start`, add `shutdown`

**Files:** `exporter/awss3exporter/exporter.go`, `exporter/awss3exporter/s3_writer.go`, `exporter/awss3exporter/factory.go`

- `exporter.go`:
  - Add field `notifier notify.Notifier` to `s3Exporter`.
  - In `start(ctx, host)`, after `e.marshaler = m`:
    ```go
    n, err := notify.New(e.config.S3Uploader.Notifications, metadata.ScopeName, e.telemetry, host, e.logger)
    if err != nil {
        return fmt.Errorf("build notifier: %w", err)
    }
    e.notifier = n
    ```
    — `newS3Exporter` already receives `exporter.Settings`; store
    `params.TelemetrySettings` on the exporter struct as
    `telemetry component.TelemetrySettings` and pass `e.telemetry`
    here. Do not introduce a separate accessor/helper for this.
  - Then call `newUploadManager(ctx, e.config, e.logger, e.signalType, m.format(), m.compressed(), e.notifier)`.
  - Add:
    ```go
    func (e *s3Exporter) shutdown(ctx context.Context) error {
        if e.notifier == nil {
            return nil
        }
        return e.notifier.Shutdown(ctx)
    }
    ```
- `s3_writer.go`: update `newUploadManager` signature to take
  `notifier notify.Notifier` and wire it with
  `upload.WithNotifier(&notifyAdapter{n: notifier})`, where
  `notifyAdapter` is a tiny local type in `s3_writer.go`:
  ```go
  type notifyAdapter struct{ n notify.Notifier }
  func (a *notifyAdapter) Enqueue(ctx context.Context, ne upload.NotifyEvent) bool {
      return a.n.Enqueue(ctx, notify.Event{Bucket: ne.Bucket, Key: ne.Key, Size: ne.Size})
  }
  ```
- `factory.go`: add `exporterhelper.WithShutdown(s3Exporter.shutdown)` to each of the three `NewLogs` / `NewMetrics` / `NewTraces` calls. Place it right after `WithStart`.

**Verification:**
- `cd exporter/awss3exporter && go build ./...`
- `cd exporter/awss3exporter && go test -race ./...`

### Step 10 — Unit tests for the notifier (the big one)

**Files:** `exporter/awss3exporter/internal/notify/notifier_test.go`

Use `httptest.Server` for live HTTP and `componenttest.NewNopTelemetrySettings()` for a telemetry setting the notifier can wire. Do not mock HTTP — spin up real `httptest` servers.

Each subtest:
1. Build a `Config` via `NewDefaultConfig()`, overriding only what's
   relevant (typically `Endpoint`, `QueueSize`, `Workers`,
   `MaxRecordsPerPost`, `MaxAttempts`, `InitialBackoff`, `MaxBackoff`).
   For tests that exercise retries, set `InitialBackoff = 1 * time.Millisecond`
   and `MaxBackoff = 10 * time.Millisecond` so the total test wall time stays short.
2. Build the notifier via `New(cfg, "test-scope", ts, host, zap.NewNop())`.
   A `componenttest.NewNopHost()` is acceptable because no auth
   extensions are referenced in these tests.
3. `t.Cleanup(func() { _ = notifier.Shutdown(ctxWithTimeout(5s)) })` to
   avoid goroutine leaks (the package's existing `TestMain` uses
   `goleak`).

Subtests (one-to-one mapping with spec tests 1–11):

1. **`TestNoop`** — `New(cfg{Endpoint:""}, …)` → assert
   `_, ok := n.(noopNotifier); ok`. `Enqueue` returns `false`.
   `Shutdown(t.Context())` returns `nil` immediately.

2. **`TestHappyPath`** — Server always 200. Enqueue 50 events, call
   `Shutdown(ctx5s)`. Assert server received 50 records total;
   `Content-Type: application/json`; a configured
   `Authorization: Bearer test-token` header is present; each record
   has URL-encoded key (use a key with `+` to verify). No dropped. No
   need to inspect metrics directly — delivery is the observable.
   (Metrics assertions are covered in tests 4, 5, 6.)

3. **`TestBatching`** — `MaxRecordsPerPost = 100`, `Workers = 1` so
   batches are deterministic. Enqueue 250 events quickly while the
   server holds the first request (via `sync.Mutex` released after a
   short sleep). Assert total records == 250, number of POSTs >= 3,
   each POST body has <= 100 records.

4. **`TestRetriesSucceed`** — Server returns 503 on first attempt,
   200 on second. Use `atomic.Int32` to count attempts per (bucket,
   key) — here, single event. Enqueue 1, shutdown with deadline.
   Assert total records delivered == 1, total POST attempts == 2.
   Read metrics via a `sdkmetric.NewManualReader` attached to a
   `sdkmetric.NewMeterProvider` built for the test; wrap in
   `component.TelemetrySettings`. Assert histogram has one sample
   with `status_class=5xx` and one with `status_class=2xx`; `sent`
   counter == 1.

5. **`TestRetriesExhausted`** — Server always 503. `MaxAttempts = 3`.
   Enqueue 1, shutdown. Assert histogram has 3 samples all
   `status_class=5xx`; `sent` == 0; `dropped{reason=retries_exhausted}` == 1.

6. **`TestPermanent4xx`** — Server returns 400. Enqueue 1, shutdown.
   Assert histogram has exactly 1 sample, `status_class=4xx`; `sent` == 0;
   `dropped{reason=permanent_4xx}` == 1.

7. **`TestQueueFull`** — `QueueSize = 2`, `Workers = 1`. Server
   blocks indefinitely via a `chan struct{}` held until the test
   releases it. Enqueue 10 events in rapid succession. Assert
   `dropped{reason=queue_full}` metric is > 0 and deliveries + drops == 10.
   (Because concurrency makes exact counts flaky: weaker assertion is
   correct here; the spec test is inherently non-deterministic.)
   Release the server, then `Shutdown` with a generous deadline.

8. **`TestShutdownDrain`** — Enqueue 100 events. Immediately call
   `Shutdown(ctx10s)`. Assert all 100 delivered (server counter).

9. **`TestShutdownDeadline`** — Server blocks for 5s per request.
   Enqueue 100 events, call `Shutdown(ctx 100ms)`. Assert `Shutdown`
   returns within ~200ms. Assert some records delivered OR dropped
   with `reason=shutdown`; total delivered + shutdown drops == 100
   (no queue_full in this config — queue size is default 10000).

10. **`TestURLEncoding`** — Key `raw/plus+char file.json`. Enqueue,
    shutdown. Assert received body decodes to a record whose
    `object.key == "raw/plus%2Bchar%20file.json"`.

11. **`TestConcurrentEnqueueDuringShutdown`** — Start 8 producer
    goroutines each calling `Enqueue` in a tight loop. After 10ms,
    call `Shutdown(ctx5s)`. Wait for producers (they exit when the
    notifier stops accepting — assert they do via `accepting.Load()`
    becoming observable; or just cap their iteration count so they
    finish). Assert no panic, no data race (run with `-race`). Rely on
    the existing `goleak` `TestMain` to catch leaks.

- Metrics-reading helper for tests 4, 5, 6: build a private
  `newTestNotifier(t, cfg, serverHandler)` that returns the notifier
  plus a `*sdkmetric.ManualReader` so tests can `reader.Collect(ctx, &rm)`
  and inspect. Add `go.opentelemetry.io/otel/sdk/metric` to the
  `awss3exporter` test imports (it is already an indirect dependency
  of the repo).

**Verification:** `cd exporter/awss3exporter && go test -race ./internal/notify/...`

### Step 11 — Exporter-level shutdown wiring test

**Files:** `exporter/awss3exporter/exporter_test.go`

- Add `TestExporterShutdownCallsNotifier`:
  - Manually construct an `s3Exporter` with a stub `notify.Notifier`
    that records `Shutdown` calls.
  - Call `e.shutdown(t.Context())`.
  - Assert the stub saw exactly one call.

- This is cheap and tests the new `shutdown` glue without re-running
  the whole HTTP stack at exporter level.

**Verification:** `cd exporter/awss3exporter && go test -race -run '^TestExporterShutdownCallsNotifier$' ./...`

### Step 12 — Config-fixture test

**Files:** `exporter/awss3exporter/testdata/config-notifications.yaml`, `exporter/awss3exporter/config_test.go`

- `testdata/config-notifications.yaml`:
  ```yaml
  receivers:
    nop:

  exporters:
    awss3:
      s3uploader:
        region: us-east-1
        s3_bucket: foo
        notifications:
          endpoint: https://example.com/webhook
          timeout: 7s
          headers:
            Authorization: Bearer abc
          queue_size: 1234
          workers: 2
          max_records_per_post: 50
          max_attempts: 5
          initial_backoff: 2s
          max_backoff: 40s

  processors:
    nop:

  service:
    pipelines:
      logs:
        receivers: [nop]
        processors: [nop]
        exporters: [awss3]
  ```
- `config_test.go`: `TestConfigNotifications`:
  - Load the fixture, cast to `*Config`, assert
    `cfg.S3Uploader.Notifications.Endpoint == "https://example.com/webhook"`,
    `QueueSize == 1234`, `Workers == 2`, `MaxRecordsPerPost == 50`,
    `MaxAttempts == 5`, `InitialBackoff == 2*time.Second`,
    `MaxBackoff == 40*time.Second`, `Timeout == 7*time.Second`,
    and `Headers["Authorization"].String() == "Bearer abc"` (note
    `confighttp.ClientConfig.Headers` is `map[string]configopaque.String`).
- `TestConfig_Validate`: add rows (endpoint non-empty + bad URL,
  endpoint + valid full config, endpoint + `queue_size=0`, endpoint +
  `max_backoff < initial_backoff`, endpoint + reserved `Content-Type`
  header, endpoint + non-empty `compression`).

**Verification:** `cd exporter/awss3exporter && go test -race -run '^(TestLoadConfig|TestConfig|TestConfigNotifications|TestConfig_Validate)$' ./...`

### Step 13 — Repo-wide verification

- `make fmt` (formats with gci as the repo requires).
- `make license-check` (new files need Apache 2.0 headers).
- `make lint` — at minimum run `cd exporter/awss3exporter && golangci-lint run` with the repo config.
- `make test` — full test suite.
- `make all` — build the collector binary to ensure builder-driven
  distribution still compiles.

## Testing Strategy

### Philosophy

- No mocks for anything that can run locally. `httptest.Server` is a
  real HTTP server — use it for all webhook tests.
- Use `sdkmetric.NewManualReader` for metric assertions rather than
  mocking the OTel SDK. The SDK is a library, and the reader is its
  testing primitive.
- No `t.Skip` on infrastructure. Every test must run every time.
- No mocked AWS S3. Existing `httptest`-backed S3 tests cover the
  upload path; the new code lives above that layer.

### Exact commands

From the exporter module directory:

- Unit (notify): `cd exporter/awss3exporter && go test -race ./internal/notify/...`
- Unit (upload): `cd exporter/awss3exporter && go test -race ./internal/upload/...`
- Unit (exporter): `cd exporter/awss3exporter && go test -race ./...`
- Lint: `cd exporter/awss3exporter && golangci-lint run` (uses the
  repo's `.golangci.yaml` by default).

From the repo root:

- `make test` — all modules.
- `make lint`
- `make check` — tests + license + lint.
- `make all` — build the distribution binary.

### Pass/fail criteria

- All unit tests pass under `-race`.
- `goleak` (already in `TestMain`) reports no leaked goroutines. This
  is the primary check that the shutdown choreography works.
- Metric assertions for tests 4, 5, 6 match the documented counts
  exactly — if they're flaky, the implementation is wrong; do not
  loosen the tests.
- `make check` returns 0.

## Risks / Considerations

1. **Direct dependency drift.** This module already carries the
   `confighttp` checksum transitively, but the implementation should add
   it as a direct `go.mod` requirement at `v0.150.0` and keep it aligned
   with the other collector-module versions already used here.
2. **`ToClient` in `start` vs `Enqueue`.** `ToClient` must be called
   during `start` (while `host` is available), not lazily. The design
   already does this — the risk is only if someone later refactors
   `notifier.New` to defer client construction.
3. **Metric cardinality.** `reason` has 4 values, `status_class` has
   4 values, `outcome` has 1 value — total cardinality is bounded.
   Safe.
4. **Shutdown race on `ch`.** The final `Shutdown` sweep reads `ch`
   after `wg.Wait`, so no worker is concurrently reading. No close
   ever happens, so producers are safe.
5. **`Enqueue` during shutdown.** `accepting.Store(false)` happens
   before `close(stopCh)`, so the producer path's drop-as-shutdown
   is strictly before any worker begins draining. Ordering is
   correct.
6. **Backoff sleep blocks shutdown.** The `select { case <-time.After: case <-ctx.Done() }` in
   `postBatch` ensures shutdown ctx unblocks a sleeping retry.
   Don't replace with bare `time.Sleep`.
7. **Headers via `ClientConfig`.** Users authenticate via
   `headers.Authorization` in YAML. `confighttp` handles this
   transparently through its RoundTripper; do not reimplement. But
   `confighttp` also overwrites colliding request headers, so the plan
   explicitly forbids `Content-Type` and `Content-Encoding`.
8. **Request compression.** `confighttp.ClientConfig` exposes request
   compression, but this feature's receiver contract is plain JSON POST.
   The plan therefore treats any configured notification compression as
   invalid rather than silently sending compressed bodies.
9. **Build graph of `internal` packages.** The `notify` package must
   not import the `upload` package; the `upload` package must not
   import `notify`. Both must not import `internal/metadata` (doing so
   would create a cycle through the exporter package in extremely
   contrived scenarios, but to be safe we keep them free). The
   exporter (root) package is the only place that knows about all
   three.
10. **`exporter.Settings.TelemetrySettings`.** Stored on `s3Exporter`
   in Step 9. Make sure `newS3Exporter` is updated; existing tests
   that build `s3Exporter` by hand (see `exporter_test.go`) must
   continue to compile — set `telemetry = componenttest.NewNopTelemetrySettings()`
   on the manual constructions or leave the field zero-valued since
   those tests never call `start`.
11. **`noopNotifier` vs nil.** `s3Exporter.notifier` is set in `start`.
    Tests that bypass `start` (like the existing `exporter_test.go`
    that wires a `testWriter`) must either also set `notifier` or
    have `shutdown` be tolerant of `nil`. The plan makes `shutdown`
    tolerant of `nil` (Step 9), which is the safer choice.
12. **`Enqueue` racing with shutdown is intentional.** `Shutdown`
    flips `accepting` to `false` before `close(stopCh)`. That means:
    - an `Enqueue` that sees `accepting=false` drops immediately with
      `reason=shutdown`, even if workers have not entered `drain()` yet;
      this is correct because shutdown has begun
    - an `Enqueue` that already observed `accepting=true` may still win
      the non-blocking send after shutdown has started; this is also
      correct because workers only switch into drain mode after
      `stopCh` is closed, and that newly queued event will be drained or
      counted during shutdown
    Do not "fix" this by adding extra locking or by trying to make the
    load/send path atomic with shutdown; the current behavior preserves
    loss accounting without blocking producers.

## Summary of decisions already baked into the plan

- `NotificationsConfig` lives in `internal/notify` (not in the root
  package) to keep `config.go` small and to avoid circular imports.
- `upload` package gets its own `EventNotifier` / `NotifyEvent` types;
  the exporter provides a thin adapter. This keeps the sub-packages
  from depending on each other.
- Scope name for metrics is `metadata.ScopeName`, passed into
  `notify.New` as a string rather than imported directly from
  `internal/metadata` inside `internal/notify`.
- `noopNotifier.Enqueue` returns `false` (consistent with "queue not
  accepted"); callers ignore the return anyway.
- Exporter-level integration test is reduced to shutdown-wiring
  verification; the upload-manager unit test already proves the
  enqueue happens after a successful upload.
- Test flakiness in "queue full" is accepted via weaker assertions
  (delivered + dropped == total, drops > 0) because the scenario is
  inherently concurrent.
