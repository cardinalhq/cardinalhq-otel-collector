# fingerprintprocessor

Cardinal uses various types of markers to track data as it flows through a collector
and (usually) into an object storage like S3.  Cost reduction is tracked using
one type of ID per telemetry type, and our indexing on top of the object store
uses these fingerprints to quickly find relevant data when using our UI or APIs
to query your stored data.

## Telemetry Types

No telemetry is deleted with this processor, but additional attributes may be
added to decorate the logs, metrics, traces, and spans for further handling
by subsequent processors, such as the aggregation processor or pitbull
processor.

### Logs

Logs are fingerprintted based on the log message itself as well as log-specific
data like the severity level.  The fingerprint code uses various techniques to
mark contents using tokens for placeholders for different data types, allowing
the shape of a log line to be represented regardless of the specific values for
those placeholders.

This fingerprint is computed once early, usually before any modifications occur,
and most of our processing use-cases rely on this fingerprint being set.  Cardinal's
managed ecosystem automatically applies this processor at the proper time.

### Metrics

Metrics are fingerprinted prior to aggregation using the attributes on the datapoints.
This is used to load balance to a specific collector instance when more than one
pod is running in the collector deployment.  This is necessary to allow aggregation
to be performed properly.

### Traces

Traces are fingerprinted in two ways:  spans and an entire trace.  Span fingerprints
are used to apply rules such as rate limiting and other head-sampling techniques to
individual spans.  Trace level fingerprints are used to do intelligent tail sampling,
where commonly repeated traces are dropped in favor of counting their numbers.

## Configuration

```yaml
processors:
  fingerprint:
    traces:
      estimator_window_size: 100
      estimator_interval: 10000
```

Default values are shown, and generally recommended, allowing for no configuration to be necessary.

The `estimator_window_size` controls the size of the window when computing an estimate for span and trace times.  Each time interval adds one entry into this list of its current estimation, and the window entries are used to estimate where the line between "slow" and "fast" traces should be placed.
The `estimator_interval` is in milliseconds and controls how often the estimate is recalculated.
