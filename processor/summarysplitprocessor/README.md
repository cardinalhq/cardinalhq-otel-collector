# summarysplitprocessor

This processor is used to convert from an older compatibility Open Telemetry format
for metric data into something closer to a modern format.  Cardinal uses This to
convert data coming in from some receivers, such as the `awsfirehose` receiver, into
standard metric data.  Removing this "summary" format in this way allows processing
standard histograms and data points instead.

## Configuration

```yaml
extensions:
  summarysplitprocessor:
```

No configuration is needed.
