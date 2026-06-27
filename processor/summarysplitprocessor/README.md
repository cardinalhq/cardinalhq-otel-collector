# summarysplitprocessor

This processor converts the older OpenTelemetry `Summary` metric type into a more
modern representation. Cardinal uses it to normalize data from some receivers, such
as the `awsfirehose` receiver. Each `Summary` metric is split into separate metrics:
`<name>.count` (a delta, non-monotonic sum), `<name>.sum` (a gauge), and one gauge
per quantile (`<name>.min`, `<name>.max`, and `<name>.quantile.<percent>`). Non-summary
metrics pass through unchanged. This lets downstream components that don't understand
the `Summary` type work with plain counter and gauge data instead.

## Configuration

```yaml
processors:
  summarysplit:

service:
  pipelines:
    metrics:
      processors: [summarysplit]
```

No configuration is needed.
