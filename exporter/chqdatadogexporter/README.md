# Datadog exporter

This exporter sends telemetry data to a Datadog endpoint.  It is a modified
version and in many cases a complete rewrite to make it consume less resources
and increase stability.

It is intended to be used as the final stage of a "sink" definition.  It can be
used outside of the Cardinal Ecosystem, but it requires additional configuration
to use properly.  When possible, use the standard Datadog exporter that is part
of the contrib release of the open telemetry receiver.

## Configuration

```yaml
exporters:
  chqdatadog/d460947f-e9d1-45f0-ac4f-130fbdf3bd2c:
    api_key: "10d6c875bf744401af0b33adc641f8a5"
    logs:
      compression: "gzip"
      endpoint: "https://datadog.intake.us-east-2.aws.cardinalhq.io"
    metrics:
      compression: "gzip"
      endpoint: "https://datadog.intake.us-east-2.aws.cardinalhq.io"
    sending_queue:
      queue_size: 20000
      storage: "file_storage/scratch"
    traces:
      compression: "gzip"
      endpoint: "https://datadog.intake.us-east-2.aws.cardinalhq.io"
```

Each endpoint is individually configured, allowing metrics or logs or traces to be independently
routed.  For an actual datadog endpoint, these are often different for each
telemetry type.

The sending_queue setting is optional but highly recommended as Datadog ingest can
take significant time periodically, and this prevents data loss.  A file-based storage scheme
used inside the cardinal pipelines to reduce memory usage.
