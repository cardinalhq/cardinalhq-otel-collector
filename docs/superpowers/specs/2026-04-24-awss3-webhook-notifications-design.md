# awss3exporter — HTTP webhook notifications on upload

**Date:** 2026-04-24
**Component:** `exporter/awss3exporter`
**Status:** Design approved; plan to follow.

## Problem

After each successful S3 upload, downstream systems (notably our `lakerunner` service, which runs a `pubsub-http` receiver) need to learn that the file exists so they can pick it up.

On native AWS, that's done via S3 → SNS → SQS (or EventBridge). This feature targets the cases where that path isn't available or practical:

- **S3-compatible stores without native event notifications.** MinIO, Ceph RGW, on-prem S3 gateways, and various object-store front-ends either don't emit S3-shaped events at all or emit them in incompatible formats.
- **Configuration complexity.** Even where S3 events exist, wiring them to a downstream consumer requires provisioning SNS topics, SQS queues, IAM policies, and cross-account/cross-region permissions. For a "just pick up my files" use case, that's a lot of moving parts.
- **Network boundaries.** Collectors deployed behind NAT, in restricted VPCs, or at edge sites often can push HTTP out to a known endpoint but can't be reached by AWS event delivery (SQS is pull-based but still needs IAM + reachability; SNS-HTTP subscriptions need a public endpoint with subscription-confirmation handshakes).

The solution is to let the collector POST a JSON notification directly to a user-configured HTTP(S) endpoint after each successful upload, using a payload shape that mimics a standard AWS S3 event notification. Receivers designed for real S3 events can consume it unchanged.

## Goals

- When `notifications.endpoint` is set in the `s3uploader` block, the exporter POSTs a notification after every successful S3 upload.
- Payload format matches the AWS S3 event shape so existing `pubsub-http`-style receivers (lakerunner today; others in the future) can consume it unchanged.
- Buffering so transient webhook slowness/outage does not stall or fail S3 uploads. Loss under extended outage is acceptable and visible via metrics.
- Retries on transient failures.
- Metrics for send timing, successes, and error/drop counts.
- The feature is disabled by default; it only activates when `notifications.endpoint` is non-empty.

## Non-goals

- Persistent/disk-backed queue. Bounded in-memory queue with drop-on-full is acceptable per the "buffering need not be 100% perfect" constraint.
- Per-event individual retry handling with priority or reordering. Whole-batch retry is sufficient because lakerunner dedupes by `(bucket, key)`.
- Framework support for protocols other than HTTP(S) JSON. (GCP Pub/Sub, Azure Event Grid, SQS direct are out of scope.)
- Batch-age trigger (time-based flush). The worker naturally batches what's already queued; a separate staleness timer is not needed.
- mdatagen scaffolding for metrics. Hand-rolled metrics under the component's existing scope are fine for this iteration.

## Receiver contract (lakerunner `pubsub-http`)

Verified in `github.com/cardinalhq/lakerunner/internal/pubsub/{http.go,parser.go}`:

- `POST <endpoint>`, `Content-Type: application/json`.
- Body ≤ 1 MB (`HTTPBodyLimitBytes`); larger returns `413`.
- HTTP layer performs no auth, signature, or Content-Type check.
- Parser auto-detects S3 / GCP / Azure Event Grid shapes. This design emits the **S3 shape** because it is the only one that natively supports multiple records per POST and because it is semantically the right fit for an S3 exporter.
- Load-bearing S3 fields consumed by the parser: `Records[].s3.bucket.name`, `Records[].s3.object.key`, `Records[].s3.object.size`. Everything else is ignored.
- The parser calls `url.QueryUnescape` on the key, so keys **must be URL-encoded** by the sender.
- Multi-record support: `parser.go` iterates `Records` and yields one `IngestItem` per record; bad records are logged and skipped without aborting the rest.
- Response classification:
  - `200` — queued successfully.
  - `4xx` (including `413`, `405`) — permanent; do not retry.
  - `5xx` and transport/timeout/connection errors — retriable.
- Dedup is by `(bucket, key)` downstream, so duplicate records from retries are safe.

## Architecture

### Components

New package: `exporter/awss3exporter/internal/notify`.

- `type Event struct { Bucket, Key string; Size int64 }` — the minimum tuple carried through the pipeline.
- `type Notifier interface { Enqueue(ctx context.Context, e Event) bool; Shutdown(ctx context.Context) error }`.
- `func New(cfg Config, telemetry component.TelemetrySettings, host component.Host, logger *zap.Logger) (Notifier, error)` — returns a `noopNotifier` when `cfg.Endpoint == ""`, otherwise the live notifier described below. Needs `host` because `confighttp.ClientConfig.ToClient(ctx, host, telemetry)` resolves auth-extension refs from host extensions.

### Ownership and wiring

- The notifier is constructed in `s3Exporter.start`, not inside the upload manager. This is where both `host` and `telemetry` are available for `confighttp.ToClient`.
- The live notifier is injected into the upload manager via a new option `upload.WithNotifier(n)`, symmetric with existing `WithACL` / `WithGCSCompatibility`.
- `factory.go` today registers only `exporterhelper.WithStart(...)` on all three signal paths (traces/metrics/logs). This design adds `exporterhelper.WithShutdown(s3Exporter.shutdown)` on every path. `shutdown` calls `notifier.Shutdown(ctx)`. Without this, the notifier's goroutines would leak on collector shutdown.
- Each signal type (`CreateLogs`, `CreateMetrics`, `CreateTraces`) constructs its own `s3Exporter`, so each gets its own notifier. No notifier is shared across signals.

### Upload-manager change

After the successful `UploadObject` in `internal/upload/writer.go`:

```go
if _, err := sw.uploader.UploadObject(ctx, uploadInput); err != nil {
    return err
}
sw.notifier.Enqueue(ctx, notify.Event{
    Bucket: overrideBucket,
    Key:    key,
    Size:   int64(content.Len()),
})
return nil
```

- `sw.notifier` is always non-nil. `notify.New` returns a `noopNotifier` when the feature is disabled, and that gets injected identically to the live one. The upload manager doesn't know the difference.
- `content.Len()` is post-compression bytes — matches what's on S3 and what lakerunner reads.
- `Enqueue`'s return value is ignored: if the queue is full, the drop is recorded by the notifier's metrics; the upload stays successful.
- The `key` is stored on the `Event` in raw form; URL-encoding is the notifier's responsibility at marshal time. Bucket names are not URL-escapable (S3 bucket charset) and are written as-is.

## Payload format

One POST carries one S3-event envelope with up to `max_records_per_post` records:

```json
{
  "Records": [
    {
      "eventSource": "aws:s3",
      "eventName":   "ObjectCreated:Put",
      "eventTime":   "2026-04-24T19:37:59Z",
      "s3": {
        "bucket": { "name": "my-bucket" },
        "object": {
          "key":  "otel-raw/uuid/collector/logs_2026-04-24T19-37-59.json.gz",
          "size": 2048
        }
      }
    }
  ]
}
```

Required (load-bearing for lakerunner): `bucket.name`, `object.key` (URL-encoded), `object.size`.

Added for compatibility with other AWS-event-shape consumers (EventBridge rules, debug tooling) and because lakerunner ignores them: `eventSource`, `eventName`, `eventTime` (RFC3339 UTC; chosen as the moment the notifier serialized the event).

At ~250 bytes per record, 100 records is ~25 KB — comfortably under lakerunner's 1 MB cap.

## Buffering and work pool

### Queue

- Bounded channel `chan Event` sized to `queue_size` (default **10000**).
- `Enqueue(ctx, e)` does a cancel-check on `ctx`, then a non-blocking `select { case ch <- e: ; default: drop }`. Upload never blocks on a slow webhook.

### Workers

- `workers` goroutines (default **4**), all pulling from the shared channel. This is the canonical Go work-pool idiom — channel receive is the atomic claim step; no separate lease/claim protocol is needed.
- Each worker runs the batch-filling loop below.
- Per-worker retry timers use independently sampled jitter so concurrent workers don't retry in lockstep (thundering herd during outage recovery).

### Batching (size-triggered, no timer)

Per worker:

```go
for {
    select {
    case <-stopCh:
        drainAndExit()                // see Shutdown
        return
    case first := <-ch:
        batch := []Event{first}
        for len(batch) < maxBatch {
            select {
            case e := <-ch:
                batch = append(batch, e)
            default:
                goto flush
            }
        }
    flush:
        postBatch(batch)              // retries as a unit
    }
}
```

Properties:

- A worker blocks until there is at least one event, so batches never flush empty.
- Under light load each POST carries 1 record.
- Under burst, or while another worker is in-flight/retrying and events queue behind it, this worker drains up to `max_records_per_post` from what's already waiting — in one syscall pattern, no timer. Precise statement: once a worker pops its first event, there is no additional batching delay. Before a worker pops, the first event can still sit behind other workers' in-flight POSTs and retries — that is expected back-pressure, not a batching problem.

Config default: `max_records_per_post = 100`.

## Retries

- `max_attempts` total attempts per batch (default **3** = 1 initial + 2 retries).
- Exponential backoff with independent per-attempt jitter: `backoff = min(initial_backoff * 2^attempt, max_backoff) * (0.5 + rand(0..1))`. Defaults `initial_backoff = 1s`, `max_backoff = 30s`.
- Retriable classes:
  - HTTP 5xx.
  - `net.Error` (includes connection refused, DNS errors, TLS handshake errors, etc.).
  - `context.DeadlineExceeded` when caused by the per-attempt timeout (not by shutdown).
- Permanent classes (drop immediately, no retry): HTTP 4xx (including `413`, `405`). Log at warn with bucket/key sample.
- Whole batch retries as a unit. Partial-retry of individual events in the batch is not supported and is not needed because lakerunner dedupes by `(bucket, key)`.
- Per-attempt context: `context.WithTimeout(shutdownAwareCtx, ClientConfig.Timeout)`. `shutdownAwareCtx` is canceled either by notifier lifecycle shutdown or by `Shutdown(ctx)`'s deadline — whichever fires first. Per-attempt fresh timeout each retry.

## Shutdown choreography

No `len(ch)` polling. No producer-visible channel close. Pattern:

1. `Shutdown(ctx)` is called by `s3Exporter.shutdown` on collector shutdown.
2. `accepting.Store(false)` is set; subsequent `Enqueue` calls fast-path to a drop.
3. `close(stopCh)` signals workers to enter drain mode.
4. Each worker's drain mode: non-blocking pulls from `ch` (batching up to `max_records_per_post`), flushing via `postBatch` on the shutdown-aware context, until `ch` yields empty OR the shutdown context deadline expires.
5. `Shutdown` waits on a `sync.WaitGroup` tracking all workers, bounded by `ctx`. If the deadline fires, it cancels the shutdown-aware context (unblocking in-flight POSTs) and counts all remaining queued events as `dropped{reason=shutdown}`.
6. The channel is never closed; producers (`Enqueue`) never observe close races.

## Metrics

Hand-rolled via `component.TelemetrySettings.MeterProvider`. Scope = the existing `ScopeName` constant in `internal/metadata/generated_status.go` (kept on the upstream path to preserve telemetry continuity with the vendored fork's source — see the vendor PR #305).

| Name | Type | Attributes | Meaning |
|---|---|---|---|
| `notifications.sent` | Counter (int) | `outcome="success"` | Successfully POSTed notifications (counted per-record, not per-batch, so rate = upload rate). |
| `notifications.dropped` | Counter (int) | `reason={queue_full,permanent_4xx,retries_exhausted,shutdown}` | Records that did not reach the receiver. Counted per-record. |
| `notifications.send.duration` | Histogram (float64 seconds) | `status_class={2xx,4xx,5xx,network_error}` | Per-POST-attempt wall latency, including retries (one histogram sample per HTTP round-trip attempt). |

`notifications.sent` and `notifications.dropped` count individual events (not batches) so operators can reason about event-level success rates directly. The histogram is per-attempt so you see retry latency distribution.

No observable queue-depth gauge in this iteration. If needed later, add `notifications.queue_depth` as an observable gauge callback reading a second atomic.

## Configuration

YAML:

```yaml
s3uploader:
  region: us-east-2
  s3_bucket: my-bucket
  # ... existing fields unchanged ...
  notifications:
    endpoint: https://lakerunner.example.com/
    timeout: 10s
    headers:
      Authorization: Bearer abc123
    # tls: ...          # optional, via confighttp
    # auth: {authenticator: ...}  # optional, via confighttp (references an extension)

    queue_size: 10000
    workers: 4
    max_records_per_post: 100
    max_attempts: 3
    initial_backoff: 1s
    max_backoff: 30s
```

Go struct in `exporter/awss3exporter/config.go`:

```go
type NotificationsConfig struct {
    confighttp.ClientConfig `mapstructure:",squash"`  // endpoint, headers, tls, auth, timeout, compression, etc.

    QueueSize         int           `mapstructure:"queue_size"`
    Workers           int           `mapstructure:"workers"`
    MaxRecordsPerPost int           `mapstructure:"max_records_per_post"`
    MaxAttempts       int           `mapstructure:"max_attempts"`
    InitialBackoff    time.Duration `mapstructure:"initial_backoff"`
    MaxBackoff        time.Duration `mapstructure:"max_backoff"`
}
```

`S3UploaderConfig` gets one new field: `Notifications NotificationsConfig \`mapstructure:"notifications"\``.

Defaults come from `confighttp.NewDefaultClientConfig()` (for transport settings like `MaxIdleConns`, `IdleConnTimeout`, `ForceAttemptHTTP2`) plus:

- `QueueSize = 10000`
- `Workers = 4`
- `MaxRecordsPerPost = 100`
- `MaxAttempts = 3`
- `InitialBackoff = 1 * time.Second`
- `MaxBackoff = 30 * time.Second`
- `ClientConfig.Timeout = 10 * time.Second` (override of confighttp default if confighttp's default differs)

### Validation

In `Config.Validate()` (existing method), when `Notifications.Endpoint != ""`:

- `Notifications.ClientConfig.Validate()` must pass (delegates to confighttp).
- `Endpoint` must parse as an `http://` or `https://` URL.
- `QueueSize >= 1`, `Workers >= 1`, `MaxRecordsPerPost >= 1`.
- `MaxAttempts >= 1`.
- `InitialBackoff > 0`, `MaxBackoff >= InitialBackoff`.

When `Notifications.Endpoint == ""`, validation skips the notifications block entirely (feature off; no required fields).

Reminder: `confighttp.ClientConfig.Validate()` does **not** require `Endpoint`, so the non-empty check must be ours.

## Testing

Unit tests in `internal/notify/`:

1. **No-op path.** `New(cfg{Endpoint:""}) → noopNotifier`; `Enqueue` returns `false` always; `Shutdown` returns immediately.
2. **Happy path.** Start notifier against an `httptest.Server` that returns 200. Enqueue N events. `Shutdown` with a generous deadline. Assert the server received the right records, URL-encoded key, `Content-Type: application/json`, and the counter matches `N`.
3. **Batching.** Enqueue 250 events quickly, `max_records_per_post=100`. Assert server received ≥3 requests, total records == 250, each body has ≤100 records.
4. **Retries succeed.** Server returns 503 on first attempt, 200 on second. Assert single event sent successfully, histogram shows two samples (one `5xx`, one `2xx`), counter reflects 1 success.
5. **Retries exhausted.** Server always returns 503. Assert counter shows `dropped{reason=retries_exhausted}=N`, histogram shows `max_attempts × N` samples.
6. **Permanent 4xx.** Server returns 400. Assert no retry (exactly 1 histogram sample), counter shows `dropped{reason=permanent_4xx}`.
7. **Queue full.** Queue size 2, workers 1, server that blocks indefinitely. Enqueue 10 events. Assert ≤2 in flight, remaining drop with `queue_full`.
8. **Shutdown drain.** Enqueue 100 events, immediately call `Shutdown(ctxWithLongDeadline)`. Assert all 100 delivered.
9. **Shutdown deadline.** Enqueue 100 events, server blocks; call `Shutdown(ctxWith100msDeadline)`. Assert some succeed, remainder counted `dropped{reason=shutdown}`, `Shutdown` returns by deadline.
10. **URL encoding.** Enqueue an event with key `"raw/plus+char file.json"`; assert server received `"raw/plus%2Bchar%20file.json"`.
11. **Concurrent Enqueue during Shutdown.** Producer goroutines hammering `Enqueue` while `Shutdown` runs; assert no panic, no leaked goroutines (via `goleak`).
12. **Config validation.** Table test covering empty endpoint (feature off), malformed URL, invalid backoff relationships, etc.

Integration smoke: add one subtest to the exporter-level test that uses the `httptest.Server` as the endpoint and asserts an S3 upload via `mock/fakes3` triggers a notification with the right `(bucket, key, size)`.

## Risks and open questions

None that block implementation. Deferred / not-now:

- Disk-backed persistent queue — explicitly out of scope; drop-on-full with metrics is sufficient.
- Observable queue-depth gauge — nice-to-have; add if the drop counter alone proves insufficient.
- Multiple endpoints / fan-out — out of scope.
- Signature-based auth (HMAC) — out of scope; bearer tokens via `headers` or auth extensions via `confighttp.auth` cover current needs.

## Summary of decisions

| Dimension | Choice |
|---|---|
| Payload shape | AWS S3 event (`Records[].s3.{bucket,object}`) |
| Buffering | Bounded in-memory channel, drop-on-full |
| Batching | Size-triggered, no timer; default 100 records/POST |
| Workers | Shared-channel work pool, default 4 |
| Retries | 3 attempts, exponential backoff 1s→30s, per-attempt independent jitter |
| Retriable errors | 5xx + transport/timeout/connection errors |
| Permanent errors | all 4xx (warn + drop) |
| Config base | `confighttp.ClientConfig` squashed into custom struct |
| Metrics | Hand-rolled, component scope, 3 instruments |
| Shutdown | `accepting` gate + `stopCh` + worker drain + WaitGroup + deadline; no channel close |
| Wiring | Notifier built in `s3Exporter.start`, injected into upload manager; `WithShutdown` added on all 3 signal paths |
