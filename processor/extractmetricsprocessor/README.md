# extracemetricsprocessor

This processor will, given rules obtained via a `chqconfig` extension, convert log content into metrics.
this allows extracting time and other information from a defined log line shape, such as HTTP server logs or other
common log outputs.

Computing metrics from logs often saves on log processing and storage costs while retaining the useful
data they contain.

## Configuration

```yaml
processors:
  extractmetrics:
    configuration_extension: chqconfig
    route: someroute

exporters:
  route/someroute: {}
```

The `route` is a receiver that the generated metrics is sent to.
