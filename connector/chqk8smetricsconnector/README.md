# chqk8smetricsconnector

This connector is part of the Cardinal Ecosystem.
Its role is to watch for incoming metrics, and emit a new time series with the
metric name prefixed to indicate the last time an item was seen, as a gauge.

This connector takes "logs" that contain Kubernetes objects and exports
metrics from them.  These metrics are useful to detect things like
pod state, configmap changes, secret changes, etc.

Additionally, changes that are noteworthy are reported to the provided
endpoint as an event.

## Configuration

```yaml
connectors:
  chqk8smetrics:
    metrics:
      interval: 60s
    events:
      interval: 5m
```

Defaults are shown.
